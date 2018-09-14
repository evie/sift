import ujson as json

from sift import logging
from sift.dataset import ModelBuilder, Model, Relations

log = logging.getLogger()

ENTITY_PREFIX = 'Q'
PREDICATE_PREFIX = 'P'

class WikidataCorpus(ModelBuilder, Model):
    @staticmethod
    def iter_item_for_line(line):
        line = line.strip()
        if line != '[' and line != ']':
            yield json.loads(line.rstrip(',\n'))

    def build(self, sc, path):
        return sc\
            .textFile(path)\
            .flatMap(self.iter_item_for_line)\
            .map(lambda i: (i['id'], i))

    @staticmethod
    def format_item(row):
        wid, item = row
        return {
            '_id': wid,
            'data': item
        }

class WikidataRelations(ModelBuilder, Relations):
    """ Prepare a corpus of relations from wikidata """
    @staticmethod
    def iter_relations_for_item(item):
        for pid, statements in item.get('claims', {}).items():
            for statement in statements:
                if statement['mainsnak'].get('snaktype') == 'value':
                    datatype = statement['mainsnak'].get('datatype')
                    if datatype == 'wikibase-item':
                        yield pid, int(statement['mainsnak']['datavalue']['value']['numeric-id'])
                    elif datatype == 'time':
                        yield pid, statement['mainsnak']['datavalue']['value']['time']
                    elif datatype == 'string' or datatype == 'url':
                        yield pid, statement['mainsnak']['datavalue']['value']

    def build(self, corpus):
        entities = corpus\
            .filter(lambda item: item['_id'].startswith(ENTITY_PREFIX))

        entity_labels = entities\
            .map(lambda item: (item['_id'], item['data'].get('labels', {}).get('en', {}).get('value', None))) \
            .filter(lambda r: r[1]) \
            .map(lambda r: (int(r[0][1:]), r[1]))

        wiki_entities = entities\
            .map(lambda item: (item['data'].get('sitelinks', {}).get('enwiki', {}).get('title', None), item['data'])) \
            .filter(lambda r: r[0]) \
            .cache()
       
        predicate_labels = corpus\
            .filter(lambda item: item['_id'].startswith(PREDICATE_PREFIX))\
            .map(lambda item: (item['_id'], item['data'].get('labels', {}).get('en', {}).get('value', None))) \
            .filter(lambda r: r[1]) \
            .cache()

        relations = wiki_entities \
            .flatMap(lambda r: ((pid, (value, r[0])) for pid, value in self.iter_relations_for_item(r[1]))) \
            .join(predicate_labels) \
            .map(lambda r: (r[1][0][0], (r[1][1], r[1][0][1])))

        return relations\
            .leftOuterJoin(entity_labels) \
            .map(lambda r: (r[1][0][1], (r[1][0][0], r[1][1] or r[0]))) \
            .groupByKey()\
            .mapValues(dict)
