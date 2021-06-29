import os

conf = {
    "application_name": os.environ.get("APPLICATION_NAME", "PermaCR-dev"),
    "wallet_file": os.environ.get(
        "WALLET_FILE",
        "arweave-key-PermaCR.json",
    ),
    "arweave_gateway": os.environ.get("ARWEAVE_GATEWAY", "https://arweave.net/"),
    "graphql_endpoint": os.environ.get("GRAPHQL_ENDPOINT", "https://arweave.net/graphql"),
    "graphql_page_size": int(os.environ.get("GRAPHQL_PAGE_SIZE", 100)),
    "tmp_upload_dir": os.environ.get("UPLOAD_DIR", "/tmp/ar_image_uploads/"),
}

if not os.path.exists(conf["tmp_upload_dir"]):
    os.makedirs(conf["tmp_upload_dir"])
