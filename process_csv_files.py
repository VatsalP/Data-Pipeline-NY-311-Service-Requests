import argparse
import logging

import apache_beam as beam
from apache_beam.options import pipeline_options


class RowTransformer(object):
    """Helper class for doing transformation"""

    def __init__(self, header):
        # Extract the feild name keys from the comma separated input.
        self.keys = self.get_csv_row(header)
        self.regex = r"^\d{2}/\d{2}/\d{4}.*"
        self.format = "%m/%d/%Y"

    def parse(self, row):
        """parses csv row into python dictionary that can be used in BigQuery"""
        values = self.get_csv_row(row)
        # convert the values to dictionary that can be used in BigQuery
        row = dict(zip(self.keys, values))
        return row

    def tranform_data(self, row):
        """Various transformations required on data rows"""
        import re
        from datetime import datetime

        for key in row.keys():
            # set empty strings to None for bigquery
            if isinstance(row[key], str):
                row[key] = row[key].strip()
                if row[key] == "":
                    row[key] = None
                    continue
            # check date and format as required by bigquery
            if re.match(self.regex, row[key]):
                row[key] = datetime.strptime(row[key][:10], self.format).isoformat()
        return row

    @staticmethod
    def get_csv_row(row_string):
        """Parse a line from csv and return it
        """
        import csv

        row_iter = iter(csv.reader([row_string]))
        return next(row_iter)


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", dest="input", required=True, help="Input file to read."
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="Output BQ table to write results to.",
    )
    parser.add_argument(
        "--fields",
        dest="fields",
        required=True,
        help="Comma separated list of field names.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    row_transformer = RowTransformer(
        header=known_args.fields,
    )

    # intialize the pipeline
    p_opts = pipeline_options.PipelineOptions(pipeline_args)

    with beam.Pipeline(options=p_opts) as pipeline:
        # read the csv file we got and skip the header
        rows = pipeline | "Read from text file" >> beam.io.ReadFromText(
            known_args.input, skip_header_lines=1
        )
        # convert csv rows string to python dictionary
        dict_records = rows | "Convert to Dictionary" >> beam.Map(
            lambda r: row_transformer.parse(r)
        )
        # transform records
        processed_record = dict_records | "Process Records" >> beam.Map(
            lambda r: row_transformer.tranform_data(r)
        )
        # write to BigQuery
        processed_record | "Write to BigQuery" >> beam.io.Write(
            beam.io.BigQuerySink(
                known_args.output,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()