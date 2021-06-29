def tags_list_to_dict(tags_list):
    tags = {}
    for tag in tags_list:
        k = tag["name"]
        v = tag["value"]
        tags[k] = v

    return tags
