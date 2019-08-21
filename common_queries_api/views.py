from django.shortcuts import render, HttpResponse
from elasticsearch import Elasticsearch
import json

# Create your views here.
ES_CONNECTION = Elasticsearch(["https://jasmin-es1.ceda.ac.uk"])


def count_files_and_dirs(request, file_path=None):
    """
    Get a count of all the files and the directories under a specified path.
    Can be use with no path.
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response

    {
        "path": "<file_path",
        "files": <int>,
        "directories": <int>
    }


    """

    # Query for fbi count under path
    fbi_query = {
        "query": {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }
    }

    # Query for dir count under path

    # Setup the response object
    response = {
        'path': file_path
    }

    if file_path:

        # Get the file count from ceda-fbi
        count = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query)
        response['files'] = count['count']

        # Get the directory count from ceda-dirs

    else:

        # Get the total count from ceda-fbi
        count = ES_CONNECTION.count(index='ceda-fbi')
        response['files'] = count['count']

        # Get the directory count from ceda-dirs


    return HttpResponse(json.dumps(response), content_type='application/json')
