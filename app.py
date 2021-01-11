
import os

from sanic import Sanic
from sanic import response
from sanic.response import json
from sanic.exceptions import InvalidUsage

from urllib.error import HTTPError

from notion.client import NotionClient
from md2notion.upload import convert as convert_md_to_notion
from md2notion.upload import uploadBlock as upload_notion_block


app = Sanic(__name__)


def create_notion_db_row(token, db_url, body='', properties=[]):
    try:
        client = NotionClient(token_v2=token)
    except HTTPError as e:
        raise AppException(
            f'Could not login to notion: {e}'
        )

    # Add new row
    cv = client.get_collection_view(db_url)
    row = cv.collection.add_row()

    errors = []

    # Set properties
    if type(properties) is not list:
        errors.append(
            f'Could not set properties `{properties}`, '
            f'because it is not a list.'
        )
        properties = []

    for prop in properties:
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

        try:
            row.set_property(prop_name, prop_value)
        except (ValueError, AttributeError) as e:
            errors.append(
                f'Could not set property: {e}'
            )

    # Convert markdown to notion and set it as the body
    notion_blocks = convert_md_to_notion(body)
    for idx, block_descriptor in enumerate(notion_blocks):
        pct = (idx + 1) / len(notion_blocks) * 100
        print(
            f'Uploading {block_descriptor["type"].__name__}, '
            f'{idx+1}/{len(notion_blocks)} ({pct:.1f}%)'
        )
        upload_notion_block(block_descriptor, row, None)

    # Return result
    result = {}
    result['status'] = 'success'
    result['added_url'] = row.get_browseable_url()
    if len(errors) > 0:
        result['errors'] = errors

    return result


@app.route('/add_db_row', methods=['POST'])
def add_db_row_handler(request):
    notion_token = os.environ.get('TOKEN')
    if notion_token is None:
        raise AppException(
            'Could not login to notion: Token was not set '
            'in environment variable `TOKEN`'
        )

    try:
        json_data = request.json
    except InvalidUsage:
        raise AppException(
            'Body was not sent in JSON format'
        )

    required_params = ['db_url']
    missing_params = set(required_params) - set(json_data.keys())
    if len(missing_params) > 0:
        raise AppException(
            f'JSON is missing required parameters: '
            f'{", ".join(missing_params)}'
        )

    body = json_data.get('body', '')
    properties = json_data.get('properties', [])
    db_url = json_data.get('db_url', None)

    result = create_notion_db_row(notion_token, db_url, body, properties)

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
