from google.cloud import datastore

KIND = 'feedSubscriptions'

client = datastore.Client()


def get_all():
    query = client.query(kind=KIND)
    return query.fetch()
