#!/usr/bin/env python

import sys, getopt
import urllib
import httplib2
import json
import pprint

conf = dict();

###################################
# http shortcut
###################################
h = httplib2.Http()

def get(url):
  global h
  (r_headers, content) = h.request(url)
  return json.loads(content)

def put(url, data):
  global h
  (r_headers, content) = h.request(url,
    "PUT",
    body=json.dumps(data))
  return json.loads(content)

def post(url, data):
  global h
  (r_headers, content) = h.request(url,
    "POST",
    body=json.dumps(data))
  return json.loads(content)

def delete(url):
  global h
  (r_headers, content) = h.request(url, "DELETE")
  return json.loads(content)

##########################################
# Mapping reader
##########################################

def get_mapping_from_file(typ):
    global conf
    with open(conf['mapping_dir']+"/"+typ+".json", 'r') as f:
        return json.loads(f.read())

##########################################
# ES
##########################################

def get_mapping_from_es(idx,tp):
    global conf
    mappings = get(conf['es_in']+"/"+idx+"/_mapping/"+tp)
    return mappings[idx]['mappings'][tp]

def set_mapping_to_es(idx,tp,mapping):
    global conf
    put(conf['es_out']+"/"+idx+"/"+tp+"/_mapping",mapping)

def get_mappings_from_es(idx):
    global conf
    mappings = get(conf['es_in']+"/"+idx+"/_mapping")
    return mappings[idx]

def set_mappings_to_es(idx,mappings):
    global conf
    put(es['es_out']+"/"+idx+"/_mapping",mappings)

def get_index_settings(idx):
    global conf
    settings = get(conf['es_in']+"/"+idx+"/_settings")
    return settings[idx];

def get_aliases(es):
    global conf
    aliases = get(es+"/_aliases")
    return aliases;

def create_alias(alias):
    global conf
    da = dict()
    da['actions'] = [];
    da['actions'].append({'add':alias});
    print da
    post(conf['es_out']+"/_aliases",da)

def create_index(idx,settings):
    global conf
    put(conf['es_out']+"/"+idx,settings);


#################################################
# Conf reader and usage
#################################################

def usage():
    u = "Get es index structure, update mapping if specified\n"
    u +="----------------"
    u += "Take a single json conf file as arg, with es_in,es_out,mapping_dir,mapping_list, update_mapping key\n"
    u +="{\n"
    u +=  "\"es_in\":\"http://es-front-a.prod.aws.rtgi.eu:9200\"\n",
    u +=  "\"es_out\":\"http://es-front-a.prod.ovh.rtgi.eu:9200\"\n",
    u +=  "\"mapping_dir\":\"~/src/radar3/conf/elasticsearch/mapping_v2\"\n",
    u +=  "\"mapping_list\":[\"dailymotion\",\"facebook\",\"forum\",\"gplus\",\"instagram\",\"press\",\"review\",\"sinaweibo\",\"twitter\",\"web\",\"youku\",\"youtube\"],\n"
    u +=  "\"update_mapping\":true,\n"
    u +=  "\"index_size\":[6,12,24,48],\n"
    u +=  "\"update_size\":true,\n"
    u +=  "\"replicas\":2,\n"
    u +=  "\"update_replicas\":true\n"
    u +="}\n"
    print u

def get_conf(argv):
    global conf
    print argv;
    with open(argv, 'r') as f:
         conf = json.loads(f.read())

########################################
# Choose index size
########################################

def select_new_size(size):
    global conf
    nsize = 0
    diff = 0
    for i in conf['index_size']:
        tsize = i
        if abs(size - tsize) < diff or diff == 0:
            nsize = tsize
            diff = abs(size - tsize)
    return nsize

def new_index_size(settings):
    global conf
    current_size = settings['settings']['index']['number_of_shards'];
    if conf['update_size']:
        return str(select_new_size(int(current_size)))
    else:
        return current_size

def build_alias_dict(idx,name,params):
    global conf;
    params['index'] = idx;
    params['alias'] = name;
    return params

##########################################
# Core
##########################################

def migrate_es_structure():
    global conf;
    indexes = get_aliases(conf['es_in'])
    for index,idx_values in indexes.items():
        #create index
        idx_settings = get_index_settings(index)
        idx_settings['settings']['index']['number_of_shards'] = new_index_size(idx_settings)
        if conf['update_replicas']:
            idx_settings['settings']['index']['number_of_replicas'] = conf['replicas']
        create_index(index,idx_settings)
        #add mapping
        if conf['update_mapping']:
            for tp in conf['mapping_list']:
                mapping = get_mapping_from_file(tp)
                set_mapping_to_es(index,tp,mapping)
        else:
            mappings = get_mappings_from_es(idx)
            set_mappings_to_es(idx,mappings)
        #add aliases
        idx_aliases = idx_values['aliases'];
        for alias,params in idx_aliases.items():
            create_alias(build_alias_dict(index,alias,params))


####################################
# Main function
####################################
def main(argv):
  get_conf(argv)
  migrate_es_structure();


if __name__ == "__main__":
    if sys.argv[1]:
        main(sys.argv[1])
    else:
        usage()
