import sys

from firebolt import ClientCredentials
from firebolt.db import connect

if len(sys.argv) != 8:
    print(
        "Usage: python create_scoped_sa.py <client_id> <client_secret> "
        "<account_name> <api_endpoint> <database> <engine> <stopped_engine>"
    )
    sys.exit(1)

client_id = sys.argv[1]
client_secret = sys.argv[2]
account_name = sys.argv[3]
api_endpoint = sys.argv[4]
database = sys.argv[5]
engine = sys.argv[6]
stopped_engine = sys.argv[7]

with connect(
    auth=ClientCredentials(client_id, client_secret),
    account_name=account_name,
    api_endpoint=api_endpoint,
) as connection:
    cursor = connection.cursor()

    cursor.execute("CREATE ROLE petro_limited_role")

    cursor.execute(f"GRANT USAGE ON DATABASE {database} TO petro_limited_role")

    cursor.execute(f"GRANT USAGE ON ENGINE {engine} TO petro_limited_role")

    cursor.execute("CREATE SERVICE ACCOUNT petro_limited_sa")

    cursor.execute(
        "CREATE USER petro_limited_user WITH SERVICE ACCOUNT "
        "petro_limited_sa ROLE petro_limited_role"
    )

    # get sercive account id and secret
    cursor.execute("CALL fb_GENERATESERVICEACCOUNTKEY('petro_limited_sa')")
    row = cursor.fetchone()
    sa_id = row[1]
    sa_secret = row[2]

    # add to github output id and secret
    print(f"::set-output name=sa_id::{sa_id}")
    print(f"::set-output name=sa_secret::{sa_secret}")
