import os
os.environ['_CONDOR_GSI_SKIP_HOST_CHECK'] = "true"

from .status import get_pool_status
from .slots import get_pool_slots
from .priorities import get_pool_priorities
from .jobs import Jobs
