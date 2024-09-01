import metis_data
from metis_data.util import monad

from tests.shared import init_state_spy


@metis_data.initialiser_register(order=1)
def state_based_init_thing(initialisation_result_state):
    init_state_spy.InitState().add_state("random-random_initialisers-1-run")
    return monad.Right('OK')
