import os
from typing import NamedTuple

from sanic import Sanic
from sanic import response
from sanic.exceptions import InvalidUsage

from urllib.error import HTTPError

from notion.client import NotionClient
from md2notion.upload import convert as convert_md_to_notion
from md2notion.upload import uploadBlock as upload_notion_block


app = Sanic(__name__)


class NotionPageProperty(NamedTuple):
    name: str
    value: str


def check_secret(request):
    correct_secret = os.environ.get('SECRET')
    if correct_secret is None:
        raise AppException(
            'Please configure password using the environment variable `SECRET`'
        )

    request_secret = request.args.get('secret', None)
    if request_secret is None:
        raise AppException(
            'Auth Failure: Please include app password using the '
            '`?secret=XXX` url query parameter.'
        )
    if request_secret != correct_secret:
        raise AppException(
            f'Auth Failure: The submitted secret query parameter '
            f'`{request_secret}` does not match the configured secret.'
        )


def parse_add_db_row_request(request):
    # Get request JSON
    try:
        json_data = request.json
    except InvalidUsage:
        raise AppException(
            'Body was not sent in JSON format'
        )

    # Verify request - required params
    required_params = ['db_url']
    missing_params = set(required_params) - set(json_data.keys())
    if len(missing_params) > 0:
        raise AppException(
            f'JSON is missing required parameters: '
            f'{", ".join(missing_params)}'
        )

    # Verify request - optional params
    db_url = json_data.get('db_url', None)
    body = json_data.get('body', '')
    property_dicts = json_data.get('properties', [])

    errors = []
    if type(property_dicts) is not list:
        errors.append(
            f'Could not set properties `{property_dicts}`, '
            f'because it is not a list.'
        )
        property_dicts = []

    # Parse properties into named tuples
    properties = []
    for prop in property_dicts:
        if type(prop) is not dict:
            errors.append(
                f'Could not set property `{prop}`, '
                f'because it is not a dictionary.'
            )
            continue
        prop_name = prop.get('name', None)
        prop_value = prop.get('value', None)

        if prop_name is None or prop_value is None:
            errors.append(
                f'Could not set property `{prop}`, '
                f'because `name` or `value` key is missing.'
            )
            continue

        properties.append(
            NotionPageProperty(prop_name, prop_value)
        )

    return (db_url, body, properties, errors)


def notion_login():
    notion_token = os.environ.get('TOKEN')
    if notion_token is None:
        raise AppException(
            'Could not login to notion: Token was not set '
            'in environment variable `TOKEN`'
        )

    try:
        notion_client = NotionClient(token_v2=notion_token)
    except HTTPError as e:
        raise AppException(
            f'Could not login to notion: {e}'
        )

    return notion_client


def notion_add_db_row(notion_client, db_url):
    db = notion_client.get_collection_view(db_url)

    print(f'Creating new row in db {db.parent.title}')
    row = db.collection.add_row()

    return row


async def notion_set_page_content(notion_client, page, body, properties):
    print(f'Setting content for page: {page.get_browseable_url()}')

    for prop in properties:
        print(f'Setting property `{prop.name}` to `{prop.value}`...')
        try:
            page.set_property(prop.name, prop.value)
        except (ValueError, AttributeError, TypeError) as e:
            print(
                f'\tERROR: Could not set property `{prop.name}`: {e}'
            )

    # Convert body markdown to notion blocks and upload to notion
    notion_blocks = convert_md_to_notion(body)
    for idx, block_descriptor in enumerate(notion_blocks):
        pct = (idx + 1) / len(notion_blocks) * 100
        print(
            f'Uploading {block_descriptor["type"].__name__}, '
            f'{idx+1}/{len(notion_blocks)} ({pct:.1f}%)'
        )
        upload_notion_block(block_descriptor, page, None)

    print(f'Finished setting content for page: {page.get_browseable_url()}')


@app.route('/add_db_row', methods=['POST'])
async def add_db_row_handler(request):
    check_secret(request)

    db_url, body, properties, errors = parse_add_db_row_request(request)

    notion_client = notion_login()
    new_row = notion_add_db_row(notion_client, db_url)

    result = {}
    result['status'] = 'background_processing'
    result['new_row_url'] = new_row.get_browseable_url()
    if len(errors) > 0:
        result['errors'] = errors

    # This part can take a LONG time and zapier and integromat time out
    # if the request takes too long.
    #
    # So we return the request after creating the page, but before
    # uploading all the content
    request.app.add_task(
        notion_set_page_content(notion_client, new_row, body, properties)
    )

    return response.json(result)


class AppException(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['status'] = 'failure'
        rv['error'] = self.message
        return rv


@app.exception(AppException)
def handle_app_exception(request, error):
    resp = response.json(error.to_dict())
    resp.status = error.status_code
    return resp


if __name__ == '__main__':
    app.debug = True
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
