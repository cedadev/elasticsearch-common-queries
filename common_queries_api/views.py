from django.http import JsonResponse
from elasticsearch import Elasticsearch
import json

# Create your views here.
ES_CONNECTION = Elasticsearch(["https://jasmin-es1.ceda.ac.uk"])


def sanitise_file_path(file_path):
    if not file_path:
        return None

    file_path_chars = list(file_path)
    file_path_chars.insert(0, '/')
    index = len(file_path_chars) - 1
    while index != 0:
        if file_path_chars[index] == '/' and file_path_chars[index] == file_path_chars[index-1]:
            del file_path_chars[index]
        index -= 1
    if file_path_chars[-1] == '/':
        del file_path_chars[-1]
    return ''.join(file_path_chars)


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
    # Sanitise file path
    file_path = sanitise_file_path(file_path)

    # Query for fbi count under path
    fbi_query = {
        "query": {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }
    }

    minimum_dirs = 1
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
            }
        }
    }

    # Setup the response object
    response = {
        'path': file_path
    }

    if file_path:
        # If there is no path, it will still return a count for directories
        minimum_dirs += file_path.count('/')
        dirs_query['query']['bool']['filter'] = [
            {
                "range": {
                    "depth": {
                        "gte": minimum_dirs
                    }
                }
            }
        ]

        # Get the total number of files and directories from ceda-fbi and ceda-dirs
        count_files = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query)
        count_dirs = ES_CONNECTION.count(index='ceda-dirs', body=dirs_query)
    else:
        # Get the total number of files and directories from ceda-fbi and ceda-dirs
        count_files = ES_CONNECTION.count(index='ceda-fbi')
        count_dirs = ES_CONNECTION.count(index='ceda-dirs')

    response['files'] = count_files['count']
    response['directories'] = count_dirs['count']

    return JsonResponse(response)


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

    # Sanitise file path
    file_path = sanitise_file_path(file_path)

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

    return JsonResponse(response)


def total_number_of_extensions(request, file_path=None):
    """
    Gets the total number of each file for each format type that exists within that path
    Works with or without a provided path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path>",
        "number_of_documents": <int>,
        "file_extensions": <list>,
    }
    """

    # Sanitise file path
    file_path = sanitise_file_path(file_path)

    after_key = ''

    # Query for fbi extensions
    fbi_query = {
        "size": 0,
        "aggs": {
            "group_by_extension": {
                "composite": {
                    "after": {
                        "extension": after_key
                    },
                    "size": 1,
                    "sources": [
                        {
                            "extension": {
                                "terms": {
                                    "field": "info.type.keyword"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }

    # Set up the response object
    response = {
        'path': file_path,
        'number_of_documents': 0,
        'file_extensions': []
    }

    if file_path:
        fbi_query['query'] = {
            "match_phrase_prefix": {
                "info.directory.analyzed": file_path
            }
        }

    # Quick query to find the number of files with info.type.keyword
    query = {
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
                        "field": "info.type.keyword"
                    }
                }
            }
        }
    }

    res = ES_CONNECTION.count(index='ceda-fbi', body=query)
    doc_count = res['count']
    response['number_of_documents'] = doc_count

    while doc_count > 0:
        # Get the extensions from ceda-fbi
        fbi_query['aggs']['group_by_extension']['composite']['after']['extension'] = after_key
        extensions = ES_CONNECTION.search(index='ceda-fbi', body=fbi_query)
        after_key = extensions['aggregations']['group_by_extension']['after_key']['extension']
        buckets = extensions['aggregations']['group_by_extension']['buckets']
        response['file_extensions'] += buckets
        for bucket in buckets:
            doc_count -= bucket['doc_count']

    return JsonResponse(response)


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

    # Sanitise file path
    file_path = sanitise_file_path(file_path)

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

    if not file_path:
        fbi_query_all['query'] = {
            "match_all": {}
        }

        fbi_query_p['query']['bool']['must'] = [
            {
                "match_all": {}
            }
        ]

    # Get the total and parameter file count from ceda-fbi
    total = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query_all)
    parameters = ES_CONNECTION.count(index='ceda-fbi', body=fbi_query_p)

    if total['count'] == 0:
        coverage = 0
    else:
        coverage = round(((parameters['count']/total['count']) * 100),2)

    response['total_files'] = total['count']
    response['parameter_files'] = parameters['count']
    response['percentage_coverage'] = coverage

    return JsonResponse(response)


def aggregate_variables(request, file_path=None):
    """
    Gets the aggregated variables under a path
    :param request: Django request object
    :param file_path: The file path prefix to query
    :return: JSON response
    {
        "path": "<file_path",
        "number_of_documents: <int>,
        "agg_variables": <dictionary>
    }
    """

    # Sanitise file path
    file_path = sanitise_file_path(file_path)

    after_key = ''

    # Query for aggregated variables from files in fbi
    fbi_query = {
        'size': 0,
        'aggs': {
            'group_by_agg_string': {
                'composite': {
                    'after': {
                        'agg_string': after_key
                    },
                    'size': 2,
                    'sources': [
                        {
                            'agg_string': {
                                'terms': {
                                    'field': 'info.phenomena.best_name.keyword'
                                }
                            }
                        }
                    ]
                }
            }
        }
    }

    # Set up the response object
    response = {
        'path': file_path,
        'number_of_documents': 0,
        'agg_variables': []
    }

    if file_path:
        fbi_query['query'] = {
                     "match_phrase_prefix": {
                         "info.directory.analyzed": file_path
                     }
                 }

    # Quick query to find the number of files with info.phenomena.agg_string
    query = {
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
                        "field": "info.phenomena.agg_string"
                    }
                }
            }
        }
    }
    res = ES_CONNECTION.count(index='ceda-fbi', body=query)
    doc_count = res['count']
    response['number_of_documents'] = doc_count

    while doc_count > 0:
        # Get the aggregated variables from ceda-fbi
        fbi_query['aggs']['group_by_agg_string']['composite']['after']['agg_string'] = after_key
        agg_variables = ES_CONNECTION.search(index='ceda-fbi', body=fbi_query)
        after_key = agg_variables['aggregations']['group_by_agg_string']['after_key']['agg_string']
        buckets = agg_variables['aggregations']['group_by_agg_string']['buckets']
        response['agg_variables'] += buckets
        for bucket in buckets:
            doc_count -= bucket['doc_count']

    return JsonResponse(response)