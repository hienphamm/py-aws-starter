import logging
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class Movies:
    """Encapsulates an Amazon DynamoDB table of movie data."""

    def __init__(self, dyn_resource) -> None:
        """
        :param dyn_resource: A Boto3 Dynamo resource.
        """
        self.dyn_resource = dyn_resource
        # The table variable is set during the scenario in the call to
        # 'exists' if the table exists. Otherwise, it is set by 'create_table'.
        self.table = None

    def exist(self, table_name) -> bool:
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
        else:
            self.table = table
        return exists

    def create_table(self, table_name) -> Any:
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the release year of the movie as the partition key and the
        title as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """

        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "year", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "title", "KeyType": "RANGE"}  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "year", "AttributeType": "N"},
                    {"AttributeName": "title", "AttributeType": "S"}
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                }
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return self.table

    def list_tables(self):
        """
        Lists the Amazon DynamoDB tables for current account.

        :return: The list of tables
        """
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)
        except ClientError as err:
            logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return tables

    def write_batch(self, movies):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_write builds a list of requests.
        On exiting the context manager, Table.batch_writer starts sending batches
        of write quests to Amazon DynamoDB and automatically handles chunking, buffering, and retrying.

        :param movies: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for movie in movies:
                    writer.put_item(Item=movie)
        except ClientError as err:
            logger.error(
                "Couldn't write to table. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def add_movie(self, title, year, plot, rating):
        """
        Adds a movie to the table.

        :param title: The title of the movie
        :param year: The release year of the movie
        :param plot: The plot summary of the movie
        :param rating: The quanlity rating of the movie
        """
        try:
            logger.info("Adding movie %s to table %s", title, self.table.name)
            self.table.put_item(
                Item={
                    "year": year,
                    "title": title,
                    "info": {
                        "plot": plot,
                        "rating": Decimal(str(rating))
                    }
                }
            )
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_movie(self, title, year):
        """
        Gets movie data from the table for a specific movie.

        :param title: The title of the movie
        :param year: The release year of the movie
        :return: The data about the requested movie
        """
        try:
            response = self.table.get_item(Key={"year": year, "title": title})
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Item"]

    def update_movie(self, title, year, rating, plot):
        """
        Updates rating and plot data for a movie in the table

        :param title: The title of the movie to update
        :param year: The release year of the movie to update
        :param rating: The updated rating to give the movie
        :param plot: The updated plot summary to give the movie
        :return: The fields that were updated, with their new values
        """
        try:
            response = self.table.update_item(
                Key={"year": year, "title": title},
                UpdateExpression="set info.rating=:r, info.plot=:p",
                ExpressionAttributeValues={
                    ":r": Decimal(str(rating)),
                    ":p": plot
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as err:
            logger.error(
                "Couldn't update movie %s in table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]

    def delete_table(self):
        """
        Deletes the table
        """
        try:
            self.table.delete()
            self.table.wait_until_not_exists()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table %s. Here's why: %s: %s",
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise


if __name__ == "__main__":
    try:
        dyn_resource = boto3.resource("dynamodb")
        movies = Movies(dyn_resource)
        table_name = "movies"

        if movies.exist(table_name):
            print(f"Table {table_name} exists.")
            movies.add_movie("The Big New Movie", 2015, "Nothing happens at all", 0.0)
            movies.add_movie("Star wars", 1977, "A long time ago in a galaxy far, far away...", 5.0)
        else:
            print(f"Table {table_name} does not exist. Creating table...")
            movies.create_table(table_name)
    except Exception as e:
        print(f"Something went wrong: {e}")
