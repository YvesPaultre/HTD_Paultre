"""
Stub for MongoDB verification script (student version).

Instructions:
- You may implement this script to inspect or verify data in your MongoDB collection.
- Use pymongo to connect and query, and print sample documents (see 'NoSQL for Data Engineers').
- This is optional but can help debug extraction issues.
"""

import pymongo
from config import DATABASE_CONFIG

client = pymongo.MongoClient(DATABASE_CONFIG['mongodb']['connection_string'])
db = client[DATABASE_CONFIG['mongodb']['database']]
collection = db['customers']

print('Sample customer documents:')
for doc in collection.find().limit(3):
    print(doc) 