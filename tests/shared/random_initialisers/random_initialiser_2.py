import metis_data
from metis_data.util import monad

from tests.shared import init_state_spy


@metis_data.initialiser_register(order=2)
def init_thing():
    init_state_spy.InitState().add_state("random-random_initialisers-2-run")
    return monad.Right('OK')
