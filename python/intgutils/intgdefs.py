""" string definitions """

REPLACE_VARS = 'replace_vars'


LISTENTRY = 'line'
LIST_FORMAT = 'format'
DEFAULT_LIST_FORMAT = 'textsp'
DEFAULT_QUERY_OUTPUT_FORMAT = 'wcl'



# IW_  (wrapper) input wcl
# OW_  (wrapper) output wcl
IW_CHECK_CMDLINE = 'check_cmdline'
IW_INPUTS = 'used'
IW_OUTPUTS = 'was_generated_by'
IW_DERIVATION = 'was_derived_from'
IW_CHECK_COMMAND = 'check_command'
IW_CMD_REQ_ARGS = 'cmd_req_args'

IW_EXEC_DEF = 'exec_def'

IW_LIST_SECT = 'list'
IW_FILE_SECT = 'filespecs'

IW_EXEC_PREFIX = 'exec_'
IW_WRAP_SECT = 'wrapper'
IW_OUTPUT_OPTIONAL = 'optional'
IW_FILE_SECT = 'filespecs'
IW_META_SECT = 'filetype_metadata'

#IW_META_HEADERS = 'headers'
#IW_META_COMPUTE = 'compute'
#IW_META_WCL = 'wcl'
#IW_UPDATE_HEAD_PREFIX = 'hdrupd_'
#IW_UPDATE_WHICH_HEAD = 'headers'
#IW_REQ_META = 'req_metadata'
#IW_OPT_META = 'opt_metadata'
OW_EXEC_PREFIX = IW_EXEC_PREFIX
OW_INPUTS = IW_INPUTS
OW_OUTPUTS = IW_OUTPUTS
OW_OUTPUTS_BY_SECT = 'outputs_by_sect'
OW_WDF = IW_DERIVATION
OW_EXEC_PREFIX = IW_EXEC_PREFIX
OW_EXEC_SECT = 'exec'
OW_PROV_SECT = 'provenance'
OW_META_SECT = 'file_metadata'
