from   util.logger import log
from   model.transfer import get_and_put


if __name__ == "__main__":
    log.info(f'Рассылка начата')
    get_and_put()
    log.info(f'Рассылка завершена\n')
