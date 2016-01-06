import ujson as json

from operator import add
from collections import Counter
from itertools import chain

from sift.dataset import DocumentModel
from sift.util import trim_link_subsection, trim_link_protocol, ngrams

import logging
log = logging.getLogger()

class EntityCounts(DocumentModel):
    """ Inlink counts """
    def __init__(self, **kwargs):
        self.threshold = kwargs.pop('threshold')
        self.filter_target = kwargs.pop('filter_target')
        super(EntityCounts, self).__init__(**kwargs)

    def build(self, corpus):
        links = corpus\
            .flatMap(lambda d: d['links'])\
            .map(lambda l: l['target'])\
            .map(trim_link_subsection)\
            .map(trim_link_protocol)\

        if self.filter_target:
            links = links.filter(lambda l: l.startswith(self.filter_target))

        return links\
            .map(lambda l: (l, 1))\
            .reduceByKey(add)\
            .filter(lambda (t, c): c > self.threshold)

    def format_items(self, model):
        return model\
            .map(lambda (target, count): {
                '_id': target,
                'count': count
            })

    @classmethod
    def add_arguments(cls, p):
        p.add_argument('--threshold', required=False, default=1, type=int)
        p.add_argument('--filter', dest='filter_target', required=False, default=None)
        return super(EntityCounts, cls).add_arguments(p)

class EntityNameCounts(DocumentModel):
    """ Entity counts by name """
    def __init__(self, **kwargs):
        self.lowercase = kwargs.pop('lowercase')
        self.filter_target = kwargs.pop('filter_target')
        super(EntityNameCounts, self).__init__(**kwargs)

    def iter_anchor_target_pairs(self, doc):
        for link in doc['links']:
            target = link['target']
            target = trim_link_subsection(target)
            target = trim_link_protocol(target)

            anchor = doc['text'][link['start']:link['stop']].strip()

            if self.lowercase:
                anchor = anchor.lower()

            if anchor and target:
                yield anchor, target

    def build(self, corpus):
        m = corpus.flatMap(lambda d: self.iter_anchor_target_pairs(d))

        if self.filter_target:
            m = m.filter(lambda (a, t): t.startswith(self.filter_target))

        return m\
            .groupByKey()\
            .mapValues(Counter)

    def format_items(self, model):
        return model\
            .map(lambda (anchor, counts): {
                '_id': anchor,
                'counts': dict(counts),
                'total': sum(counts.itervalues())
            })

    @classmethod
    def add_arguments(cls, p):
        p.add_argument('--filter', dest='filter_target', required=False, default=None)
        p.add_argument('--lowercase', dest='lowercase', required=False, default=False, action='store_true')
        return super(EntityNameCounts, cls).add_arguments(p)

class NamePartCounts(DocumentModel):
    """
    Occurrence counts for ngrams at different positions within link anchors.
        'B' - beginning of span
        'E' - end of span
        'I' - inside span
        'O' - outside span
    """
    def __init__(self, **kwargs):
        self.lowercase = kwargs.pop('lowercase')
        self.filter_target = kwargs.pop('filter_target')
        self.max_ngram = kwargs.pop('max_ngram')
        super(NamePartCounts, self).__init__(**kwargs)

    def iter_anchors(self, doc):
        for link in doc['links']:
            anchor = doc['text'][link['start']:link['stop']].strip()
            if self.lowercase:
                anchor = anchor.lower()
            if anchor:
                yield anchor

    @staticmethod
    def iter_span_count_types(anchor, n):
        parts = list(ngrams(anchor, n, n))
        if parts:
            yield parts[0], 'B'
            yield parts[-1], 'E'
            for i in xrange(1, len(parts)-1):
                yield parts[i], 'I'

    def build(self, corpus):
        part_counts = corpus\
            .flatMap(self.iter_anchors)\
            .flatMap(lambda a: chain.from_iterable(self.iter_span_count_types(a, i) for i in xrange(1, self.max_ngram+1)))\
            .map(lambda p: (p, 1))\
            .reduceByKey(add)\
            .map(lambda ((term, spantype), count): (term, (spantype, count)))

        part_counts += corpus\
            .flatMap(lambda d: ngrams(d['text'], self.max_ngram))\
            .map(lambda t: (t, 1))\
            .reduceByKey(add)\
            .filter(lambda (t, c): c > 1)\
            .map(lambda (t, c): (t, ('O', c)))

        return part_counts\
            .groupByKey()\
            .mapValues(dict)\
            .filter(lambda (t, cs): 'O' in cs and len(cs) > 1)

    def format_items(self, model):
        return model\
            .map(lambda (term, part_counts): {
                '_id': term,
                'counts': dict(part_counts)
            })

    @classmethod
    def add_arguments(cls, p):
        p.add_argument('--filter', dest='filter_target', required=False, default=None)
        p.add_argument('--lowercase', dest='lowercase', required=False, default=False, action='store_true')
        p.add_argument('--max-ngram', dest='max_ngram', required=False, default=2, type=int)
        return super(NamePartCounts, cls).add_arguments(p)

class EntityInlinks(DocumentModel):
    """ Inlink sets for each entity """
    def build(self, corpus):
        return corpus\
            .flatMap(lambda d: ((d['_id'], l) for l in set(l['target'] for l in d['links'])))\
            .mapValues(trim_link_subsection)\
            .mapValues(trim_link_protocol)\
            .map(lambda (k, v): (v, k))\
            .groupByKey()\
            .mapValues(list)

    def format_items(self, model):
        return model\
            .map(lambda (target, inlinks): {
                '_id': target,
                'inlinks': inlinks
            })

class EntityVocab(EntityCounts):
    """ Generate unique indexes for entities in a corpus. """
    def __init__(self, **kwargs):
        self.min_rank = kwargs.pop('min_rank')
        self.max_rank = kwargs.pop('max_rank')
        super(EntityVocab, self).__init__(**kwargs)

    def build(self, corpus):
        log.info('Building entity vocab: df rank range=(%i, %i)', self.min_rank, self.max_rank)
        m = super(EntityVocab, self)\
            .build(corpus)\
            .map(lambda (target, count): (count, target))\
            .sortByKey(False)\
            .zipWithIndex()\
            .map(lambda ((df, t), idx): (t, (df, idx)))

        if self.min_rank != None:
            m = m.filter(lambda (t, (df, idx)): idx >= self.min_rank)
        if self.max_rank != None:
            m = m.filter(lambda (t, (df, idx)): idx < self.max_rank)
        return m

    def format_items(self, model):
        return model\
            .map(lambda (term, (f, idx)): {
                '_id': term,
                'count': f,
                'rank': idx
            })

    @staticmethod
    def load(sc, path, fmt=json):
        log.info('Loading entity-index mapping: %s ...', path)
        return sc\
            .textFile(path)\
            .map(fmt.loads)\
            .map(lambda r: (r['_id'], (r['count'], r['rank'])))

    @classmethod
    def add_arguments(cls, p):
        p.add_argument('--min-rank', dest='min_rank', required=False, default=0, type=int, metavar='MIN_RANK')
        p.add_argument('--max-rank', dest='max_rank', required=False, default=int(1e5), type=int, metavar='MAX_RANK')
        return super(EntityVocab, cls).add_arguments(p)

class EntityComentions(DocumentModel):
    """ Entity comentions """
    @staticmethod
    def iter_unique_links(doc):
        links = set()
        for l in doc['links']:
            link = trim_link_subsection(l['target'])
            link = trim_link_protocol(link)
            if link not in links:
                yield link
                links.add(link)

    def build(self, corpus):
        return corpus\
            .map(lambda d: (d['_id'], list(self.iter_unique_links(d))))\
            .filter(lambda (uri, es): es)

    def format_items(self, model):
        return model\
            .map(lambda (uri, es): {
                '_id': uri,
                'entities': es
            })

class MappedEntityComentions(EntityComentions):
    """ Entity comentions with entities mapped to a numeric index """
    def __init__(self, **kwargs):
        self.entity_vocab_path = kwargs.pop('entity_vocab_path')
        super(MappedEntityComentions, self).__init__(**kwargs)

    def prepare(self, sc):
        log.info('Preparing mapped entity vocab...')
        ev = EntityVocab\
            .load(sc, self.entity_vocab_path)\
            .map(lambda (term, (count, rank)): (term, rank))
        self.ev = sc.broadcast(dict(ev.collect()))
        return super(MappedEntityComentions, self).prepare(sc)

    def build(self, corpus):
        return super(MappedEntityComentions, self)\
            .build(corpus)\
            .map(lambda (uri, es): (uri, [self.ev.value[e] for e in es if e in self.ev.value]))\
            .filter(lambda (uri, es): es)

    @classmethod
    def add_arguments(cls, p):
        super(MappedEntityComentions, cls).add_arguments(p)
        p.add_argument('entity_vocab_path', metavar='ENTITY_VOCAB_PATH')
        return p
