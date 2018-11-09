from estnltk import logger
from estnltk.storage import PostgresStorage


def tag_collection(tagger, args):
    logger.setLevel(args.logging)
    logger.info('start script')

    storage = PostgresStorage(dbname=args.dbname,
                              user=args.user,
                              host=args.host,
                              port=args.port,
                              pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    collection = storage.get_collection(args.collection)

    overwrite = (args.mode == 'overwrite')

    try:
        collection.create_layer_buffered(tagger=tagger,
                                         overwrite=overwrite,
                                         progressbar=args.progressbar
                                         )
    except Exception as e:
        logger.error(e)
        exit(1)
    finally:
        storage.close()

    logger.info('end script')
