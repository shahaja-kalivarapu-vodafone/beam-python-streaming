import apache_beam as beam
import argparse
import collections
import google.auth
import google.auth.transport.requests
import json
import logging
import requests
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.options.pipeline_options import PipelineOptions
from requests.packages.urllib3.util.retry import Retry
from typing import List

# os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
# os.environ["SSL_CERT_FILE"] = certifi.where()

ENDPOINT = "https://europe-west1-aiplatform.googleapis.com/v1/projects/3645786829022/locations/europe-west1/endpoints/408786988276348739:predict"
tealium_url = 'https://collect.tealiumiq.com/event'
schema = 'page_url:STRING,cart_remove:STRING,page_title:STRING,product_view:STRING,cart_id:STRING,page_name:STRING,' \
         'checkout_complete:STRING,product_brand:STRING,transaction_product_brand:STRING,journey_type:STRING,' \
         'product_name:STRING,cart_open:STRING,device:STRING,cart_view:STRING,checkout_start:STRING,' \
         'checkout_step1:STRING,checkout_step2:STRING,cart_add:STRING,event_label:STRING,event_category:STRING,' \
         'event_action:STRING,cart_product_brand:STRING,event_value:STRING,product_category:STRING,visitor_type:STRING,' \
         'transaction_complete:STRING,data_dom_domain:STRING,data_dom_title:STRING,data_dom_pathname:STRING,' \
         'data_udo_tealium_library_name:STRING,data_udo_tealium_visitor_id:STRING,data_udo_tealium_timestamp_utc:STRING,' \
         'data_udo_tealium_library_version:STRING,data_udo_tealium_session_id:STRING,event_id:STRING,' \
         'udo_device_type:STRING,udo_tealium_event_type:STRING,udo_device:STRING,udo_timestamp:STRING,' \
         'data_udo_device:STRING,data_udo_tealium_profile:STRING,data_udo_tealium_account:STRING,' \
         'data_udo_tealium_datasource:STRING,visitor_id:STRING'


class ParsePubSubMessageDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        logging.info(f'type of element and content {element}, {type(element)}')
        message_data = element.data
        data_json = message_data.decode("utf-8").replace("'", '"')
        try:
            data_json = json.loads(data_json)
            flatten_data = self.flatten(data_json)
            # logging.info(f'type of flatten_data and content {flatten_data}, {type(flatten_data)}')
            message_attributes = element.attributes
            # logging.info(f'type of message_attributes and content {message_attributes}, {type(message_attributes)}')
            selected_attributes_list = ["page_url", "cart_remove", "page_title", "product_view", "cart_id", "page_name",
                                        "checkout_complete", "product_brand", "transaction_product_brand", "journey_type",
                                        "product_name", "cart_open", "device", "cart_view", "checkout_start",
                                        "checkout_step1",
                                        "checkout_step2", "cart_add", "event_label", "event_category", "event_action",
                                        "cart_product_brand", "event_value", "product_category", "visitor_type",
                                        "transaction_complete"]
            selected_attributes = {k: message_attributes[k] for k in selected_attributes_list if k in message_attributes.keys()}

            selected_data_list = ["event_id", "visitor_id", "data_dom_domain", "data_dom_title",
                                  "data_dom_pathname", "data_udo_ut_visitor_id",
                                  "data_udo_tealium_library_name", "data_udo_tealium_session_id",
                                  "data_udo_tealium_visitor_id", "data_udo_tealium_timestamp_utc",
                                  "data_udo_tealium_library_version", "data_udo_device",
                                  "data_udo_tealium_profile", "data_udo_tealium_datasource",
                                  "data_udo_tealium_account"]

            selected_data = {k: flatten_data[k] for k in selected_data_list if k in flatten_data.keys()}
            output = {**selected_attributes, **selected_data}
            # logging.info(f'type of output and content {output}, {type(output)}')
            bq_row = self.map_to_bq_row(output)
            logging.info(f'mapped to BQ row: {bq_row}, {type(bq_row)}')
            yield bq_row
        except json.JSONDecodeError:
            logging.info(f'empty pubsub message received')

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def map_to_bq_row(self, _dict):
        output_row = {}
        list_keys = []
        schema_keys = schema.split(',')
        for item in schema_keys:
            list_keys.append(item.split(':', 1)[0])
        for key in list_keys:
            if key in _dict.keys():
                output_row[key] = _dict[key]
            else:
                output_row[key] = ''
        return output_row


class GeneratePredictionsDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        payload = {"instances": [element]}
        id_token = self.generate_token()
        headers = {'Authorization': f'Bearer {id_token}', 'Content-Type': 'application/json'}
        session_aib = requests.Session()
        req = session_aib.post(url=ENDPOINT, json=payload, headers=headers)
        session_aib.close()
        try:
            logging.info(f'predictions from the model {req.json()}, {type(req)}')
            request_payload = req.json()['predictions'][0]
            if 'bd_visitor_id' in request_payload.keys():
                tealium_data = self.tealium_request(request_payload)
                # logging.info(f'Tealium data from tealium call: {tealium_data}, {type(tealium_data)}')
                yield tealium_data
        except json.JSONDecodeError:
            logging.info(f'empty response returned')

    def tealium_request(self, payload):
        tealium_data = {
            "tealium_datasource": "hgdvhg",
            "tealium_profile": "personal-cdp-sandbox",
            "tealium_account": "personal",
            "bd_visitor_id": payload["bd_visitor_id"],
            "bd_propensity_score": payload["bd_propensity_score"]
        }
        # logging.info(f'tealium_data is {tealium_data}, {type(tealium_data)}')
        return tealium_data

    def generate_token(self):
        creds, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        return creds.token


class SendtoTealiumDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        logging.info(f'Tealium data for tealium req: {element}, {type(element)}')
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5
        )

        session_tealium = requests.Session()
        session_tealium.verify = False
        # try:
        tealium_req = session_tealium.post(tealium_url, json=element)
        session_tealium.close()
        logging.info(f'tealium_req returned {tealium_req.text}, {tealium_req.status_code}')
        # except requests.exceptions.ConnectionError:
        #     logging.info(f'Tealium connection refused')
        yield


def _logging(row):
    logging.info(f'logging BQ message {row}, {type(row)} ')
    return row


def run(
    input_subscription: str,
    output_table: str,
    beam_args: List[str] = None
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    project, table_id = output_table.split(':')
    dataset, table = table_id.split('.')

    with beam.Pipeline(options=options) as pipeline:
        messages = (pipeline
                    | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription,
                                                                    with_attributes=True)
                    | "Parse Pubsub Message" >> beam.ParDo(ParsePubSubMessageDoFn())
                    )
        _ = (messages
             | "Generate Predictions" >> beam.ParDo(GeneratePredictionsDoFn())
             | "Send score to Tealium" >> beam.ParDo(SendtoTealiumDoFn())
             )
        (messages
            # | "logging before write to BQ" >> beam.Map(_logging)
            | "reshuffle" >> beam.Reshuffle()
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(table=table, project=project, dataset=dataset,
                                                             schema=schema,
                                                             write_disposition=
                                                             beam.io.BigQueryDisposition.WRITE_APPEND,
                                                             create_disposition=beam.io.BigQueryDisposition.
                                                             CREATE_NEVER,
                                                             insert_retry_strategy=RetryStrategy.
                                                             RETRY_ON_TRANSIENT_ERROR, batch_size=50,
                                                             ignore_unknown_columns=True)
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
             "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--error_table",
        help="Output BigQuery table for error rows specified as: "
             "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
             '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    args, beam_args = parser.parse_known_args()
    logging.info(f'--input_subscription is {args.input_subscription}')
    logging.info(f'--output_table is {args.output_table}')
    logging.info(f'beam_args is {beam_args}')

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        beam_args=beam_args,
    )
