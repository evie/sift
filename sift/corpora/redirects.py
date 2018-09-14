import ujson as json

from sift import logging
from sift.dataset import Model, DocumentModel

log = logging.getLogger()

class MapRedirects(Model):
    """ Map redirects """
    def __init__(self, *args, **kwargs):
        self.from_path = kwargs.pop('from_path')
        self.to_path = kwargs.pop('to_path')

    def prepare(self, sc):
        return {
            "from_rds": self.load(sc, self.from_path).cache(),
            "to_rds": self.load(sc, self.to_path).cache()
        }

    @staticmethod
    def map_redirects(source, target):
        return source \
            .map(lambda r: (r[1], r[0])) \
            .leftOuterJoin(target) \
            .map(lambda r: (r[1][0], r[1][1] or r[0])) \
            .distinct()

    def build(self, from_rds, to_rds):
        # map source of destination kb
        # e.g. (a > b) and (a > c) becomes (b > c)
        mapped_to = to_rds\
            .leftOuterJoin(from_rds) \
            .map(lambda r: (r[1][1] or r[0], r[1][0])) \
 \
        # map target of origin kb
        # e.g. (a > b) and (b > c) becomes (a > c)
        mapped_from = from_rds \
            .map(lambda r: (r[0], r[1])) \
            .leftOuterJoin(mapped_to) \
            .map(lambda r: (r[1][0], r[1][1])) \
            .filter(lambda r: r[1])

        rds = (mapped_from + mapped_to).distinct()
        rds.cache()

        log.info('Resolving transitive mappings over %i redirects...', rds.count())
        rds = self.map_redirects(rds, rds)

        log.info('Resolved %i redirects...', rds.count())
        return rds

    @staticmethod
    def load(sc, path, fmt=json):
        log.info('Using redirects: %s', path)
        return sc\
            .textFile(path)\
            .map(fmt.loads)\
            .map(lambda r: (r['_id'], r['target']))

    def format_items(self, model):
        return model \
            .map(lambda r: {
            '_id': r[0],
            'target': r[1]
            })

    @classmethod
    def add_arguments(cls, p):
        super(MapRedirects, cls).add_arguments(p)
        p.add_argument('from_path', metavar='FROM_REDIRECTS_PATH')
        p.add_argument('to_path', metavar='TO_REDIRECTS_PATH')
        return p

class RedirectDocuments(DocumentModel):
    """ Map links in a corpus via a set of redirects """
    def __init__(self, **kwargs):
        self.redirect_path = kwargs.pop('redirects_path')
        super(RedirectDocuments, self).__init__(**kwargs)

    def prepare(self, sc):
        params = super(RedirectDocuments, self).prepare(sc)
        params['redirects'] = self.load(sc, self.redirect_path).cache()
        return params

    def build(self, corpus, redirects):
        articles = corpus.map(lambda d: (d['_id'], d))

        def map_doc_links(doc, rds):
            for l in doc['links']:
                l['target'] = rds[l['target']]
            return doc

        return corpus\
            .map(lambda d: (d['_id'], set(l['target'] for l in d['links']))) \
            .flatMap(lambda r: [(t, r[0]) for t in r[1]]) \
            .leftOuterJoin(redirects) \
            .map(lambda r: (r[1][0], (r[0], r[1][1] if r[1][1] else r[0]))) \
            .groupByKey()\
            .mapValues(dict)\
            .join(articles) \
            .map(lambda r: map_doc_links(r[1][1], r[1][0]))

    def format_items(self, model):
        return model

    @classmethod
    def add_arguments(cls, p):
        super(RedirectDocuments, cls).add_arguments(p)
        p.add_argument('redirects_path', metavar='REDIRECTS_PATH')
        return p
