from biothings.web.handlers import MetadataSourceHandler


class NDESourceHandler(MetadataSourceHandler):
    """
    GET /metadata
    GET /v1/metadata
    """

    def extras(self, _meta):

        _meta['sourcesInfo'] = {}

        # for s, d in self.biothings.config.THING.items():
        #     if 'tax_id' in d:
        #         _meta['taxonomy'][s] = int(d['tax_id'])
        #     if 'assembly' in d:
        #         _meta['genome_assembly'][s] = d['assembly']

        return _meta
