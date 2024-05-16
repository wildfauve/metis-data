import metis_job
from metis_job.util import monad

from tests.shared import init_state_spy


@metis_job.initialiser_register(order=1)
def init_thing():
    init_state_spy.InitState().add_state("random-random_initialisers-1-run")
    return monad.Right('OK')
