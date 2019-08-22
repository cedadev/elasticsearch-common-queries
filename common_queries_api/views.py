from django.shortcuts import render, HttpResponse
from elasticsearch import Elasticsearch
import json

# Create your views here.
ES_CONNECTION = Elasticsearch(["https://jasmin-es1.ceda.ac.uk"])


def sanitize_file_path(file_path):
    if file_path != None:
        new = list(file_path)
        new.insert(0, '/')
        index = len(new) - 1
        while index != 0:
            if new[index] == '/' and new[index] == new[index - 1]:
                del new[index]
            index -= 1
        print(''.join(new))
        return ''.join(new)
    else:
        return None


def count_files_and_dirs(request, file_path=None):
    """
    Get a count of all the files and the directories under a specified path.
    Can be used with no path.
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "files": <int>,
        "directories": <int>
    }
    """
    # Sanitize file path
    file_path = sanitize_file_path(file_path)

    # Query for fbi count under path
    fbi_query = {
        "query": {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }
    }

    # Query for dirs count under path
    dirs_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "prefix": {
                            "archive_path.keyword": {
                                "value": file_path
                            }
                        }
                    }
                ],
                "filter": {
                    "range": {
                        "depth": {
                            "gte": file_path.count('/') + 1
                        }
                    }
                }
            }
        }
    }

    # Setup the response object
    response = {
        'path': file_path
    }

    if file_path:
        # Get the total number of files and directories from ceda-fbi and ceda-dirs
        count_files = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query)
        count_dirs = ES_CONNECTION.count(index='ceda-dirs', body=dirs_query)
    else:
        # Get the total number of files and directories from ceda-fbi and ceda-dirs
        count_files = ES_CONNECTION.count(index='ceda-fbi')
        count_dirs = ES_CONNECTION.count(index='ceda-dirs')

    response['files'] = count_files['count']
    response['directories'] = count_dirs['count']

    return HttpResponse(json.dumps(response), content_type='application/json')


def total_size_of_files(request, file_path=None):
    """
    Gets the current total size of all files under the path
    Works with or without a provided path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "total_file_size": <int>,
    }
    """
    # Sanitize file path
    file_path = sanitize_file_path(file_path)

    # Query for fbi total size under path
    fbi_query = {
        "size": 0,
        "aggs": {
            "total_size": {
                "sum": {
                    "field": "info.size"
                }
            }
        }
    }

    # Setup the response object
    response = {
        'path': file_path
    }

    if file_path:
        fbi_query['query'] = {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }

    # Get the total file and directory size from ceda-fbi
    total_file_size = ES_CONNECTION.search(index='ceda-fbi', body=fbi_query)

    response['total_file_size'] = total_file_size['aggregations']['total_size']['value']

    return HttpResponse(json.dumps(response), content_type='application/json')


def total_number_of_formats(request, file_path=None):
    """
    Gets the total number of each file for each format type that exists within that path
    Works with or without a provided path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "formats": <dictionary>,
    }
    """
    # Sanitize file path
    file_path = sanitize_file_path(file_path)

    # Query for fbi formats
    fbi_query = {
        "size": 0,
        "aggs": {
            "file_formats": {
                "terms": {
                    "field": "info.format.keyword"
                }
            }
        }
    }

    # Set up the response object
    response = {
     'path': file_path
    }

    if file_path:
        fbi_query['query'] = {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }

    # Get the total formats and counts from ceda-fbi
    formats = ES_CONNECTION.search(index='ceda-fbi', body=fbi_query)

    response['formats'] = formats['aggregations']['file_formats']['buckets']

    return HttpResponse(json.dumps(response), content_type='application/json')


def aggregate_variables(request, file_path=None): #TIME's OUT WHEN LARGE DIRECTORY
    """
    Gets the aggregated variables under a path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "variables": <dictionary>
    }
    """
    # Sanitize file path
    file_path = sanitize_file_path(file_path)

    # Query for aggregated variables from files in fbi
    fbi_query = {
        'size': 0,
        'aggs': {
            'agg_variables': {
                'terms': {
                    'field': 'info.phenomena.agg_string'
                }
            }
        }
    }

    # Set up the response object
    response = {
        'path': file_path
    }

    if file_path:
        fbi_query['query'] = {
                     "match_phrase_prefix": {
                         "info.directory.analyzed": file_path
                     }
                 }

    # Get the aggregated variables from ceda-fbi
    variables = ES_CONNECTION.search(index='ceda-fbi', body=fbi_query)

    response['agg_variables'] = variables['aggregations']['agg_variables']['buckets']

    return HttpResponse(json.dumps(response), content_type='application/json')


def coverage_by_handlers(request, file_path=None):
    """
    Finds the coverage by handlers by calculating the % of all files that have parameters
    Works with or without a provided path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "total_files": <int>,
        "parameter_files": <int>,
        "percentage_coverage": <float>
    }
    """
    # Sanitize file path
    file_path = sanitize_file_path(file_path)

    # Query for total files from path in fbi
    fbi_query_all = {
        "query": {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }
    }

    # Query for parameter files from path in fbi
    fbi_query_p = {
      "query": {
        "bool": {
          "must": [
            {
              "match_phrase_prefix": {
              "info.directory.analyzed": file_path
              }
            }
          ],
          "filter": {
            "exists": {
              "field": "info.phenomena"
            }
          }
        }
      }
    }

    # Set up the response object
    response = {
        'path': file_path
    }

    if file_path:
        # Get the total and parameter file count from ceda-fbi
        total = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query_all)
        parameters = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query_p)
    else:
        # Get the total and parameter file count from ceda-fbi
        total = ES_CONNECTION.count(index='ceda-fbi')
        parameters = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query_p)

    coverage = (parameters['count']/total['count']) * 100

    response['total_files'] = total['count']
    response['parameter_files'] = parameters['count']
    response['percentage_coverage'] = coverage

    return HttpResponse(json.dumps(response), content_type='application/json')