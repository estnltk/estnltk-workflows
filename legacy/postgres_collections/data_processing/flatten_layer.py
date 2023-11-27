from estnltk_workflows.postgres_collections import parse_args

args = parse_args('collection', 'pgpass', 'host', 'port', 'dbname', 'user', 'schema', 'role',
                  'mode', 'progressbar', 'logging', 'input_layer', 'output_layer', 'output_attributes',
                  description='Run FlattenTagger on EstNltk PostgreSQL collection.')


from estnltk_workflows.postgres_collections import tag_collection
from estnltk.taggers import FlattenTagger


tagger = FlattenTagger(input_layer=args.input_layer,
                       output_layer=args.output_layer,
                       output_attributes=args.output_attributes)

tag_collection(tagger, args)
