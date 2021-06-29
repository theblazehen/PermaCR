import os
import json


from resources.util import tags_list_to_dict

from conf import conf

import arweave
from arweave.transaction_uploader import get_uploader

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


class Organization:
    def __init__(self, org):
        self._conf = conf

        self._wallet = arweave.Wallet(self._conf.get("wallet_file"))

        self.org = org

        try:
            gql_transport = RequestsHTTPTransport(url=conf.get("graphql_endpoint"))
            self._gql_client = Client(transport=gql_transport)
        except Exception as e:
            raise Exception(f"Failed to establish graphql connection: {e}")

        self._get_org_info()

    def _get_org_info(self):
        self.exists = False
        self.authorized_users = []
        self.homepage = ""

        cursor = ""
        complete = False
        while not complete:
            query = gql(
                """
                query {
                  transactions(
                    tags: [
                      { name: "app", values: ["%s"] }
                      { name: "type", values: ["org"] }
                      { name: "org", values: ["%s"] }
                    ]
                    sort: HEIGHT_ASC
                    first: %s
                    after: "%s"
                  ) {
                    edges {
                      cursor
                      node {
                        owner {
                          address
                        }
                        tags {
                          name
                          value
                        }
                      }
                    }
                  }
                }
            """
                % (self._conf.get("application_name"), self.org, conf['graphql_page_size'], cursor)
            )
            res = self._gql_client.execute(query)
            print(res)

            if len(res["transactions"]["edges"]) > 0:
                if len(res['transactions']['edges']) != conf['graphql_page_size']: complete = True # We didn't get the page size, therefore we have all the data

                cursor = res['transactions']['edges'][-1]['cursor']
                self.exists = True

                # Start off with an initial authorized user of the user who created the org
                self.authorized_users = [res["transactions"]["edges"][0]["node"]["owner"]["address"]]

                self.homepage = ""  # Start with a blank homepage link

                # Ordered from first tx to last
                for tx in [x["node"] for x in res["transactions"]["edges"]]:
                    tags = tags_list_to_dict(tx["tags"])
                    tx_authorized_users = json.loads(tags["authorized_users"])

                    # Security check:
                    # Ensure the owner of the tx has access to the organization
                    if tx["owner"]["address"] not in self.authorized_users:
                        continue

                    self.authorized_users = tx_authorized_users
                    self.homepage = tags.get("homepage", self.homepage)
            else:
                complete = True

    def create(self):
        if self.exists:
            self.update()
        else:
            if len(self.authorized_users) == 0:
                self.authorized_users.append(self._wallet.address)
            self.update()
            self.exists = True

    def update(self):
        if self._wallet.address not in self.authorized_users:
            print("Not authorized")
            return False

        tx = arweave.Transaction(wallet=self._wallet)
        tx.add_tag("app", self._conf["application_name"])
        tx.add_tag("type", "org")
        tx.add_tag("org", self.org)
        tx.add_tag("authorized_users", json.dumps(self.authorized_users))
        tx.add_tag("homepage", self.homepage)

        tx.sign()
        tx.send()
        return True


class ArweaveBackend:
    def __init__(self):
        self._conf = conf

        try:
            self._wallet = arweave.Wallet(conf.get("wallet_file"))
        except:
            raise Exception("Could not load wallet file")

        try:
            gql_transport = RequestsHTTPTransport(url=conf.get("graphql_endpoint"))
            self._gql_client = Client(transport=gql_transport, fetch_schema_from_transport=True)
        except Exception as e:
            raise Exception(f"Failed to establish graphql connection: {e}")

    def set_tag_ref(self, organization, image_name, tag_name, ref):
        org = Organization(organization)
        if self._wallet.address not in org.authorized_users:
            print("Not authorized")
            return False

        tx = arweave.Transaction(wallet=self._wallet)
        tx.add_tag("app", self._conf["application_name"])
        tx.add_tag("type", "ref")
        tx.add_tag("org", organization)
        tx.add_tag("image_name", image_name)
        tx.add_tag("tag_name", tag_name)
        tx.add_tag("ref", ref)
        tx.sign()
        tx.send()
        return tx.id

    def get_tag_ref(self, organization, image_name, tag_name):

        org = Organization(organization)

        edges = []
        cursor = ""
        complete = False
        while not complete:
            query = gql(
                """
                query {
                transactions(
                    tags: [
                    { name: "app", values: ["%s"] }
                    { name: "type", values: ["ref"] }
                    { name: "image_name", values: ["%s"] }
                    { name: "tag_name", values: ["%s"] }
                    ]
                    owners: %s
                    sort: HEIGHT_DESC
                    first: %s
                    after: "%s"
                ) {
                    edges {
                    cursor
                    node {
                        owner {
                        address
                        }
                        tags {
                        name
                        value
                        }
                    }
                    }
                }
                }

            """
                % (
                    self._conf.get("application_name"),
                    image_name,
                    tag_name,
                    json.dumps(org.authorized_users),
                    conf['graphql_page_size'],
                    cursor,
                )
            )
            res = self._gql_client.execute(query)
            print(res)

            cur_edges = res["transactions"]["edges"]
            edges.extend(cur_edges)

            print(len(cur_edges))
            if len(cur_edges) > 0: cursor = cur_edges[-1]['cursor']
            if len(cur_edges) != conf['graphql_page_size']: complete = True

        # Filter by those who are valid, as a double check
        txs = [tx["node"] for tx in edges if tx["node"]["owner"]["address"] in org.authorized_users]

        if len(txs) > 0:
            # The reference exists
            tags_list = txs[0]["tags"]
            tags = tags_list_to_dict(tags_list)
            return tags.get("ref")
        else:
            return False

    def get_content_url(self, organization, image_name, ref):
        org = Organization(organization)

        edges = []
        cursor = ""
        complete = False
        while not complete:
            query = gql(
                """
                query {
                transactions(
                    tags: [
                    { name: "app", values: ["%s"] }
                    { name: "type", values: ["content"] }
                    { name: "image_name", values: ["%s"] }
                    { name: "ref", values: ["%s"] }
                    ]
                    owners: %s
                    sort: HEIGHT_DESC
                    first: %s
                    after: "%s"
                ) {
                    edges {
                    cursor
                    node {
                        owner {
                        address
                        }
                        id
                    }
                    }
                }
                }
            """
                % (
                    self._conf.get("application_name"),
                    image_name,
                    ref,
                    json.dumps(org.authorized_users),
                    conf['graphql_page_size'],
                    cursor,
                )
            )
            res = self._gql_client.execute(query)
            print(res)

            cur_edges = res["transactions"]["edges"]
            edges.extend(cur_edges)

            if len(cur_edges) > 0: cursor = cur_edges[-1]['cursor']
            if len(cur_edges) != conf['graphql_page_size']: complete = True

        # Filter by those who are valid
        txs = [tx["node"] for tx in edges if tx["node"]["owner"]["address"] in org.authorized_users]

        if len(txs) > 0:
            txid = txs[0]["id"]
            return self._conf["arweave_gateway"] + txid
        else:
            return False

    def upload_manifest(self, organization, image_name: str, ref: str, data: bytes):
        org = Organization(organization)
        if self._wallet.address not in org.authorized_users:
            print("Not authorized")
            return False

        # Add to manifest cache so the blob HEAD after a push will work
        tx = arweave.Transaction(wallet=self._wallet, data=data)

        tx.add_tag("app", self._conf["application_name"])
        tx.add_tag("type", "content")
        tx.add_tag("org", organization)
        tx.add_tag("image_name", image_name)
        tx.add_tag("ref", ref)

        manifest = json.loads(data)
        tx.add_tag("Content-Type", manifest["mediaType"])

        tx.sign()
        tx.send()

        return tx.id

    def upload_file(self, organization, image_name, ref, file_path):
        org = Organization(organization)
        if self._wallet.address not in org.authorized_users:
            print("Not authorized")
            return False

        with open(file_path, 'rb', buffering=0) as file_handler:
            tx = arweave.Transaction(wallet=self._wallet, file_handler=file_handler, file_path=file_path)

            tx.add_tag("app", self._conf["application_name"])
            tx.add_tag("type", "content")
            tx.add_tag("Content-Type", "application/octet-stream")
            tx.add_tag("org", organization)
            tx.add_tag("image_name", image_name)
            tx.add_tag("ref", ref)
            tx.sign()
            uploader = get_uploader(tx, file_handler)

            while not uploader.is_complete:
                uploader.upload_chunk()

                print(f"Upload file: {organization}/{image_name}:{ref}: Chunks {uploader.uploaded_chunks}/{uploader.total_chunks}")

            return tx.id

        os.remove(file_path)
