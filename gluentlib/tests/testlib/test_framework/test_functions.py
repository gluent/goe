#! /usr/bin/env python3
""" Functions used in both "test --setup", "test_runner" and "test_setup".
    Allows us to share code but also keep scripts trim and healthy.
    LICENSE_TEXT
"""

import re
import threading
import time

from gluent import get_log_fh, get_log_fh_name, get_offload_options, get_options, init, \
    log as offload_log, normal,\
    OFFLOAD_OP_NAME, verbose, vverbose
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.offload.column_metadata import match_table_column
from gluentlib.offload.offload_constants import DBTYPE_ORACLE
from gluentlib.offload.offload_functions import convert_backend_identifier_case, data_db_name
from gluentlib.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from gluentlib.schema_sync.schema_sync_constants import SCHEMA_SYNC_OP_NAME
from gluentlib.util.misc_functions import substitute_in_same_case
from schema_sync import get_schema_sync_opts
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from tests.testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory
from test_sets.stories.story_globals import STORY_SET_ALL


_QUOTE = {"'": "|'", "|": "||", "\n": "|n", "\r": "|r", '[': '|[', ']': '|]', '\x1b': '|0x001B'}


def add_common_test_options(opt_object):
    """ Options common to test runs and test setup.
        Trivial but keeps help text/defaults consistent between old 'test' script and new scripts.
    """
    opt_object.add_option('--filter', dest='filter', default='.*')
    opt_object.add_option('--teamcity', dest='teamcity', action='store_true',
                          help='Emit TeamCity integration messages')
    opt_object.add_option('--test-user', dest='test_user', default='SH_TEST', help='Defaults to SH_TEST')
    opt_object.add_option('--test-pass', dest='test_pass', default=None)
    opt_object.add_option('--test-hybrid-pass', dest='test_hybrid_pass', default=None)


def add_common_test_run_options(opt_object):
    """ Options shared by new and old scripts used when running tests (not setup).
        Trivial but keeps help text/defaults consistent between old 'test' script and new scripts.
    """
    opt_object.add_option('--run-blacklist-only', dest='run_blacklist_only', action='store_true', default=False,
                          help='Only run blacklisted tests, which are skipped by default.')
    opt_object.add_option('--test-pq-degree', dest='test_pq_degree', help='Number of PQ slaves used for parallel tests')


def add_story_test_options(opt_object):
    """ Trivial but keeps help text/defaults consistent between old 'test' script and new scripts. """
    opt_object.add_option('--story', dest='story', default=None,
                          help='Use in combination with --set=stories: %s, or csv of stories'
                          % '|'.join(_ for _ in sorted(STORY_SET_ALL)))
    opt_object.add_option('--list-stories', dest='list_stories', action='store_true', default=False,
                          help='View a list of stories')


def add_test_set_options(opt_object, sets=None):
    """ Trivial but keeps help text/defaults consistent between old 'test' script and new scripts. """
    if sets is None:
        sets = test_constants.ALL_TEST_SETS
    opt_object.add_option('--set', dest='test_set',
                   help='%s, or csv of sets, or leave unset for full'
                   % '|'.join(_ for _ in sorted(sets)))


def get_backend_db_table_name_from_metadata(hybrid_schema, hybrid_view, repo_client):
    """ Use metadata to get correct case for db name/table, returned as a tuple """
    hybrid_metadata = repo_client.get_offload_metadata(hybrid_schema, hybrid_view)
    assert hybrid_metadata, f'Missing hybrid metadata for: {hybrid_schema}.{hybrid_view}'
    return hybrid_metadata.backend_owner, hybrid_metadata.backend_table


def get_backend_columns_for_hybrid_view(hybrid_schema, hybrid_view, backend_api, repo_client):
    backend_db, backend_table = get_backend_db_table_name_from_metadata(hybrid_schema, hybrid_view, repo_client)
    backend_columns = backend_api.get_columns(backend_db, backend_table)
    return backend_columns


def get_backend_testing_api(options, config=None, do_not_connect: bool=False, no_caching=False):
    messages = OffloadMessages.from_options(options, get_log_fh())
    if not config:
        config = OrchestrationConfig.from_dict({'verbose': options.verbose,
                                                'vverbose': options.vverbose,
                                                'execute': options.execute})
    return backend_testing_api_factory(config.target, config, messages, dry_run=bool(not options.execute),
                                       no_caching=no_caching, do_not_connect=do_not_connect)


def get_frontend_testing_api(options, config=None, do_not_connect: bool=False, trace_action=None):
    messages = OffloadMessages.from_options(options, log_fh=get_log_fh())
    if not config:
        config = OrchestrationConfig.from_dict({'verbose': options.verbose,
                                                'vverbose': options.vverbose,
                                                'execute': options.execute})
    return frontend_testing_api_factory(config.db_type, config, messages, dry_run=bool(not options.execute),
                                        do_not_connect=do_not_connect, trace_action=trace_action)


def get_data_db_for_schema(schema, config):
    return convert_backend_identifier_case(config, data_db_name(schema, config))


def get_lines_from_log(search_text, search_from_text='', max_matches=None, file_name_override=None) -> list:
    """ Searches for text in the test logfile starting from the start of the
        story in the log or the top of the file if search_from_text is blank and
        returns all matching lines (up to max_matches).
    """
    log_file = file_name_override or get_log_fh_name()
    if not log_file:
        return []
    # We can't log search_text otherwise we put the very thing we are searching for in the log
    start_found = False if search_from_text else True
    matches = []
    lf = open(log_file, 'r')
    for line in lf:
        if not start_found:
            start_found = (search_from_text in line)
        else:
            if search_text in line:
                matches.append(line)
                if max_matches and len(matches) >= max_matches:
                    return matches
    return matches


def get_line_from_log(search_text, search_from_text='') -> str:
    matches = get_lines_from_log(search_text, search_from_text=search_from_text, max_matches=1)
    return matches[0] if matches else None


def get_orchestration_options_object(operation_name=OFFLOAD_OP_NAME, log_path=None, verbose=None,
                                     vverbose=None, execute=True, force=True):
    if operation_name == OFFLOAD_OP_NAME:
        tmp_opt = get_options(operation_name=OFFLOAD_OP_NAME)
        get_offload_options(tmp_opt)
    elif operation_name == SCHEMA_SYNC_OP_NAME:
        tmp_opt = get_schema_sync_opts()
    else:
        raise NotImplementedError('Unknown operation_name: %s' % operation_name)

    # Use [] as we don't want to parse the args passed in to test
    orchestration_options, _ = tmp_opt.parse_args([])
    init(orchestration_options)
    orchestration_options.log_path = log_path
    orchestration_options.verbose = verbose
    orchestration_options.vverbose = vverbose
    orchestration_options.execute = execute
    orchestration_options.force = force
    return orchestration_options


def get_naughty_state():
    return (3, (867845712, 2517630297, 4113802262, 3832845409, 4274770220, 2933308597, 2069026353, 1608262008, 1175025829, 136239185, 798549531, 799836310, 4173874443, 1888131572, 3266689661, 2463967463, 674146465, 973130765, 1381478306, 2635451420, 277223063, 1325833064, 1790687856, 3477140222, 3694711644, 1564747662, 1131939538, 2825326370, 558708389, 2867676132, 2582166872, 2550524937, 801968977, 1630264611, 680646674, 646558933, 851633012, 2334560710, 2637115945, 2836788286, 106618271, 537576597, 553682966, 1865745650, 985546814, 417742147, 3935217365, 1876403660, 1145810952, 1966366701, 72448140, 1500580031, 404317749, 3140213456, 3718998014, 1828714325, 3709526722, 2459673971, 257159296, 3508218124, 628274885, 1964138653, 958781628, 436073236, 760899514, 1422568901, 563228272, 1418861625, 412185357, 2882090829, 2201732512, 2556467958, 1316275486, 2839203926, 1975328693, 1194327448, 2230151648, 1600088209, 3031722755, 281337523, 2263837204, 2293027541, 844461942, 658259102, 2938541840, 3008125606, 461811676, 3729916019, 2489755419, 506669042, 2213465887, 2964919587, 3129397207, 1049334739, 3970972215, 34231123, 246758470, 1994433461, 2684585420, 3551745102, 1626305063, 3648707488, 4087674575, 709904385, 3194471281, 2711728285, 3698303124, 3321213222, 3704269131, 2025616447, 94606515, 4160638393, 2525505954, 1888703187, 2402356367, 621906962, 516357590, 641639996, 845805447, 2600931881, 3407746736, 2625477482, 3557993654, 871095334, 2470007987, 1600156948, 4014611419, 3983250958, 3062432192, 1477504699, 453985769, 845714079, 2051146265, 2525994769, 3733456017, 3351165442, 4021399604, 1039281293, 3592658998, 1334280807, 482095312, 1074806973, 273656125, 1542182508, 1898829808, 1950386529, 3718513403, 2968151679, 4272287909, 1710800990, 944066947, 3197950669, 1325389986, 2285662539, 1604791535, 1179777838, 251612605, 3453556951, 1571710058, 2254737125, 1266347288, 4075037033, 3618268330, 1765800347, 1967899063, 4012907389, 3408585750, 3710417972, 1471979419, 611196154, 2081194827, 1243592498, 684749033, 504939024, 140625192, 67259772, 3103057314, 522275695, 950709687, 2436555421, 523104326, 2780840987, 1191823720, 2474933086, 3583259878, 2726262959, 358580988, 1915897166, 1560424919, 2403855019, 3028549311, 2724864766, 834109916, 2961881794, 2256966212, 1745188437, 1403172930, 101687620, 1228268092, 1700171642, 2471990687, 236747376, 1008774658, 1001399747, 1639997664, 851793658, 1305688034, 1750160113, 2213228726, 2521282511, 1025749479, 161610440, 1787640269, 1345975292, 1604416100, 4033852757, 1478180602, 2831416122, 1617479633, 2304956108, 3444285339, 4072618094, 2607610481, 1084763637, 2541000430, 111976412, 3277031890, 3010677146, 2849013551, 1831550725, 4089826671, 2777532896, 2213133531, 3260258821, 2034522965, 2768416512, 2198110185, 1618205915, 1969914833, 2229204032, 3107012390, 12479816, 1076898461, 4014351018, 2085925954, 486007476, 799994390, 2772397475, 4146368929, 258888957, 774024782, 2168401371, 1762065518, 723092825, 4272381005, 3604992564, 1862294247, 2862765010, 1342331596, 996729396, 1486084589, 2926276278, 3219674574, 2182046214, 24801156, 3247922264, 284101738, 3993771661, 3359441069, 3164315499, 2373256634, 3884058381, 3002779750, 2873439052, 3942266218, 1906246142, 470294221, 3104678, 2067885068, 2783495220, 864310994, 3663713963, 1553535597, 1612542150, 3269722244, 4077077579, 2143950209, 2136980988, 39275533, 2639542437, 1240863445, 27400231, 1352265642, 3169145259, 634182554, 1099328374, 4192861085, 3876573856, 663399466, 1544641673, 1437593215, 973955375, 998675277, 975543114, 3292653426, 3808027706, 3825143159, 2430661847, 2095305586, 3145503544, 4045925779, 289339989, 86767268, 1504443491, 2572483361, 1846156476, 3998893737, 2233308997, 1387972879, 2316828218, 3879698180, 1761908971, 1529853927, 2163182841, 2622418498, 1326230478, 3371233799, 3058952273, 787280406, 2421273622, 1474623708, 2136366755, 3343818265, 685201427, 2282631397, 1312260307, 1860727510, 137922540, 617529964, 3895421419, 50199918, 4181747118, 2598360587, 3660544849, 3666723637, 3745297546, 2666585758, 1821565744, 3141603121, 2296553478, 231358109, 1225469573, 2719633821, 1243621809, 558509393, 156481005, 2675150406, 3638543391, 2228675064, 1710151815, 1521545672, 2101907547, 2322574617, 68034341, 3970994572, 1891499154, 2013559193, 111879487, 390851650, 3256563599, 4060604333, 1841085631, 3705718601, 1347323727, 3354455793, 3198154258, 3554602100, 322796215, 3368952942, 2810722212, 1497892661, 1357781957, 2826144083, 279283552, 1004488882, 871079641, 832819745, 2874449640, 392000592, 3826685324, 1505910016, 3136767072, 1109999932, 3329515559, 2172438906, 2036391645, 1262285744, 1287667181, 2384752652, 2727927604, 247248187, 1663269740, 3436920447, 3801264021, 1254490679, 648537, 3931381728, 1416682836, 1026114187, 349891101, 3849072714, 4023156033, 2868451396, 408905070, 291752388, 2862965334, 1607471637, 4062471209, 2863484936, 3256743224, 2904675794, 3037069795, 2882302520, 3307447809, 181090370, 369786122, 3084894688, 2394141865, 692852986, 2500636331, 2671349644, 93635900, 4067995909, 855928688, 894783667, 1945268540, 2416204753, 2608131587, 2358733700, 2796706253, 3256513933, 139196365, 1256373013, 3015986987, 3324606915, 2341050130, 1600795530, 2251673703, 1293254668, 917411905, 1064865981, 2096092418, 1507513897, 2095308604, 1467666505, 2765964705, 3750108478, 2424289826, 727428252, 3678420042, 1069923987, 1621861499, 3192679748, 3576706212, 4103768584, 1372497050, 1220969076, 1732928666, 2465183388, 3168907521, 1940216664, 2674080485, 3131920502, 3528994384, 60221386, 2688206247, 3849792559, 2571580402, 2071813402, 2929330341, 2153687177, 1579838105, 2859199406, 1186152099, 550683397, 1081454457, 2865037624, 2023952057, 3589401384, 4040422173, 2005820836, 1388604986, 2447678626, 3295910072, 3711365716, 3423770177, 3436211278, 3388508992, 2193288553, 157807187, 764801110, 1548239363, 2218406803, 3638358814, 3274868648, 4199746182, 3194120663, 486702372, 1380096852, 2439814883, 1017302012, 1060079655, 2415047258, 2674741902, 2280775487, 3129449610, 2365931261, 750960713, 1716136290, 250046033, 3778587405, 3247458837, 2310884448, 1569696318, 2584974173, 1124598445, 3301495125, 2690742740, 2888108148, 813145934, 327452719, 1407137901, 805690238, 1232445752, 3496432878, 419556645, 1947653945, 1106430028, 3525392820, 153785155, 2924344747, 60347495, 550581327, 2378361772, 499578897, 3972679580, 1764356628, 3845257083, 2782636942, 1852647679, 2309904063, 806810160, 2741079007, 1228544742, 1121060175, 1429512239, 2549463029, 456464930, 1056500197, 2752635718, 694065055, 501584551, 1889173034, 2548543245, 889931711, 1815270013, 2645882345, 3733784286, 2202078988, 1741443779, 1191384522, 1279042822, 1532839753, 2216194177, 2514361287, 2680602395, 3875954945, 2191821407, 1756113807, 2995448234, 4254531833, 3974082506, 1229392016, 4118553104, 2146435944, 2009255117, 3232525182, 1891491060, 532837646, 55609364, 298360391, 3157174730, 3314062378, 759100213, 2172677533, 80683600, 3901514421, 1240287807, 3593747686, 2524317, 1990126099, 751029187, 826202304, 1393362968, 2779343187, 1379981549, 250551051, 3226733064, 928054329, 3514772522, 1618187221, 484648122, 1030802313, 3542834498, 3879290959, 1962368240, 2790093067, 1716519541, 3898880103, 84012799, 1331058819, 1588431061, 1401978573, 316788888, 298), None)


def get_test_set_sql_path(directory_name, db_type=None):
    db_type = db_type or DBTYPE_ORACLE
    return f'test_sets/{directory_name}/sql/{db_type}'


def gl_wide_max_columns(frontend_api, backend_api_or_count):
    if backend_api_or_count:
        if isinstance(backend_api_or_count, (int, float)):
            backend_count = backend_api_or_count
        else:
            backend_count = backend_api_or_count.gl_wide_max_test_column_count()
        if backend_count:
            return min(backend_count, frontend_api.gl_wide_max_test_column_count())
        else:
            return frontend_api.gl_wide_max_test_column_count()
    else:
        return frontend_api.gl_wide_max_test_column_count()


def log(line: str, detail: int=normal, ansi_code=None):
    """Write log entry but without Redis interaction."""
    offload_log(line, detail=detail, ansi_code=ansi_code, redis_publish=False)


def minus_column_spec_count(owner_a, table_name_a, owner_b, table_name_b, desc, cursor=None, frontend_api=None,
                            log_diffs_if_non_zero=False):
    """ Check that all columns are of the same spec when comparing table A and table B (likely a Hybrid View).
        Requires either a cursor (legacy) or FrontendApi.
    """
    def exec_sql(sql, fetch_all=False):
        if cursor:
            return cursor.execute(sql).fetchall() if fetch_all else cursor.execute(sql).fetchone()
        else:
            return frontend_api.execute_query_fetch_all(sql) if fetch_all else frontend_api.execute_query_fetch_one(sql)

    def column_spec_oracle_sql(owner, table_name):
        tab_col_sql_template = """SELECT data_type, CASE WHEN data_type LIKE 'TIMESTAMP%%' THEN NULL else data_length
                               END AS data_length, data_precision, data_scale FROM dba_tab_columns WHERE owner = '{}'
                               AND table_name = '{}'"""
        q = tab_col_sql_template.format(owner.upper(), table_name.upper())
        return q

    assert cursor or frontend_api
    q1 = column_spec_oracle_sql(owner_a.upper(), table_name_a.upper())
    q2 = column_spec_oracle_sql(owner_b.upper(), table_name_b.upper())
    q = 'SELECT COUNT(*) FROM (%s MINUS %s)' % (q1, q2)
    row_count = exec_sql(q)[0]
    if row_count != 0 and log_diffs_if_non_zero:
        log('column_spec_a: {}'.format(exec_sql(q1, fetch_all=True)), detail=verbose)
        log('column_spec_b: {}'.format(exec_sql(q2, fetch_all=True)), detail=verbose)

    return row_count


def normalise_test_pass_options(opt_object):
    """ Hardcode default passwords if not supplied
        Not interfering with case of passwords to honour settings defined in TeamCity
    """
    if not opt_object.test_pass:
        opt_object.test_pass = opt_object.test_user
    if hasattr(opt_object, 'test_hybrid_pass') and not opt_object.test_hybrid_pass:
        opt_object.test_hybrid_pass = to_hybrid_schema(opt_object.test_user)


def response_time_bench(test, desc, bench_key, fn):
    before = time.time()
    try:
        return fn()
    finally:
        test.response_time(bench_key, max(time.time() - before, 0.001))


def test_passes_filter(test_name, test_name_re, test_options, known_failure_blacklist):
    """ Returns whether to run this test based on the filter regexp and blacklist.
        Slightly complex logic because we only want to log blacklisted skips if they would have otherwise run.
    """
    if test_options.run_blacklist_only:
        return test_name in known_failure_blacklist and test_name_re.search(test_name)
    elif test_name_re.search(test_name):
        m = re.search(r'(.*)_pq\d$', test_name)
        if m is not None:
            test_name = m.group(1)
        if test_name.lower() in [_.lower() for _ in known_failure_blacklist]:
            if not test_options.teamcity:
                log(test_name + ': skipping blacklisted test')
            return False
        return True
    return False


def table_minus_row_count(frontend_api, owner_a, table_name_a, owner_b, table_name_b,
                          column=None, parallel=0, where_clause=None, tz=None):
    """ Duplicate of story_assertion_functions table_minus_row_count() but frontend_api based. """
    if not column:
        if frontend_api.lobs_support_minus_operator():
            column = '*'
        else:
            column = frontend_api.lob_safe_table_projection(owner_a, table_name_a)
    where_clause = where_clause or ''
    q1 = 'SELECT %s FROM %s.%s %s' % (column, owner_a, table_name_a, where_clause)
    q2 = 'SELECT %s FROM %s.%s %s' % (column, owner_b, table_name_b, where_clause)
    q = 'SELECT COUNT(*) FROM (%s MINUS %s)' % (q1, q2)
    query_options = frontend_api.test_time_zone_query_option(tz) if tz else {}
    return frontend_api.execute_query_fetch_one(q, query_options=query_options, log_level=VVERBOSE)[0]


def test_data_host_compare_no_hybrid_schema(test, frontend_schema, frontend_table_name,
                                            backend_schema, backend_table_name,
                                            frontend_api, backend_api, column_csv=None):
    """ Compare data in a CSV of columns or all columns of a table when there is no hybrid schema.
        We load frontend and backend data into Python sets and use minus operator.
        Because of variations in data types returned by the assorted frontend/backend clients all
        date based columns are converted to strings in SQL.
    """
    def fix_numeric_variations(v, column):
        """ Convert any values like '.123' or '-.123' to '0.123' or '-0.123' """
        if column.is_number_based() and isinstance(v, str):
            if v.startswith('-.'):
                return '-0.{}'.format(v[2:])
            elif v.startswith('.'):
                return '0.{}'.format(v[1:])
            elif v and v.lower() == 'nan':
                return 'NaN'
            elif v and v.lower() == 'inf':
                return 'Inf'
            elif v and v.lower() == '-inf':
                return '-Inf'
            else:
                return v
        else:
            return v

    def preprocess_data(data, columns):
        new_data = [fix_numeric_variations(d, col) for row in data for d, col in zip(row, columns)]
        return set(new_data)

    fe_owner_table = frontend_api.enclose_object_reference(frontend_schema, frontend_table_name)
    be_owner_table = backend_api.enclose_object_reference(backend_schema, backend_table_name)
    fe_columns = frontend_api.get_columns(frontend_schema, frontend_table_name)
    fe_id_column = match_table_column('ID', fe_columns)
    be_columns = backend_api.get_columns(backend_schema, backend_table_name)
    be_id_column = match_table_column('ID', be_columns)

    if column_csv:
        # We've been asked to verify specific columns
        fe_columns = [match_table_column(_, fe_columns) for _ in column_csv.split()]

    # Validate the columns one at a time otherwise it is too hard to unpick which ones have problems
    for validation_column in fe_columns:
        if validation_column.is_nan_capable():
            # TODO For the moment it is proving too difficult to validate float/double data
            #      The results coming back from different systems are sometimes rounded, sometimes in scientific
            #      notation. Plus NaN/Inf/-Inf handling is problematic. For now I've excluded from validation.
            continue

        log('Checking {}'.format(validation_column.name), detail=verbose)
        fe_validation_columns = [validation_column]
        be_validation_columns = [match_table_column(validation_column.name, be_columns)]
        if validation_column.name.upper() != 'ID':
            # Always include ID column to help us locate issues
            fe_validation_columns = [fe_id_column] + fe_validation_columns
            be_validation_columns = [be_id_column] + be_validation_columns

        fe_projection = frontend_api.host_compare_sql_projection(fe_validation_columns)
        be_projection = backend_api.host_compare_sql_projection(be_validation_columns)
        frontend_sql = f'SELECT {fe_projection} FROM {fe_owner_table}'
        backend_sql = f'SELECT {be_projection} FROM {be_owner_table}'
        frontend_data = preprocess_data(frontend_api.execute_query_fetch_all(frontend_sql, log_level=VERBOSE),
                                        fe_validation_columns)
        backend_data = preprocess_data(backend_api.execute_query_fetch_all(backend_sql, log_level=VERBOSE),
                                       be_validation_columns)
        base_minus_backend = list(frontend_data - backend_data)
        backend_minus_base = list(backend_data - frontend_data)
        if base_minus_backend != [] or backend_minus_base != []:
            # Extra logging to help diagnose mismatches
            log('Base minus backend count: %s' % len(base_minus_backend), detail=verbose)
            log('Backend minus base count: %s' % len(backend_minus_base), detail=verbose)
            log('Base minus backend (first 10 rows only): %s' % str(sorted(base_minus_backend)[:11]), detail=vverbose)
            log('Backend minus base (first 10 rows only): %s' % str(sorted(backend_minus_base)[:11]), detail=vverbose)
        test.assertEqual(base_minus_backend, [],
                         'Extra ' + frontend_schema + ' results (cf ' + backend_schema + ') for SQL:\n' + frontend_sql)
        test.assertEqual(backend_minus_base, [],
                         'Extra ' + backend_schema + ' results (cf ' + frontend_schema + ') for SQL:\n' + backend_sql)


def to_hybrid_schema(base_schema):
    # Don't modify the text case coming out of this function, it is used for sh_test pwd which is case sensitive.
    m = re.search('_H$', base_schema.upper())
    if m is not None:
        return base_schema
    else:
        return substitute_in_same_case('%s_H', base_schema if base_schema else '')


def teamcity_escape(s):
    return "".join(_QUOTE.get(x, x) for x in s)


def test_teamcity_starttestsuite(options, name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_starttestsuite(name)
        print("##teamcity[testSuiteStarted name='%s']" % name, flush=True)


def test_teamcity_starttestsuite_pq(options, name, flow_id=None):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        if not flow_id:
            flow_id = name
        print("##teamcity[flowStarted flowId='%s']" % flow_id)
        print("##teamcity[testSuiteStarted name='%s' flowId='%s']" % (name, flow_id))
        return flow_id


def test_teamcity_endtestsuite(options, name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_endtestsuite(name)
        print("##teamcity[testSuiteFinished name='%s']" % name, flush=True)


def test_teamcity_endtestsuite_pq(options, name, flow_id):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        print("##teamcity[testSuiteFinished name='%s' flowId='%s']" % (name, flow_id))
        print("##teamcity[flowFinished flowId='%s']" % flow_id)


def test_teamcity_starttestblock(options, name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_starttestblock(name)
        print("##teamcity[blockOpened name='%s']" % name, flush=True)


def test_teamcity_endtestblock(options, name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_endtestblock(name)
        print("##teamcity[blockClosed name='%s']" % name, flush=True)


def test_teamcity_starttest(options, test_name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_starttest(test_name)
        print("##teamcity[testStarted name='%s' captureStandardOutput='true']" % test_name, flush=True)


def test_teamcity_starttest_pq(options, test_name, parent_flow_id):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        flow_id = threading.current_thread().native_id
        print("##teamcity[flowStarted name='%s' flowId='%s' parent='%s']" % (test_name, flow_id, parent_flow_id))
        print("##teamcity[testStarted name='%s' flowId='%s' captureStandardOutput='true']" % (test_name, flow_id))


def test_teamcity_stdout(options, test_name, message):
    if options.teamcity:
        print("##teamcity[testStdOut name='%s' out='%s']" % (test_name, teamcity_escape(message)), flush=True)


def test_teamcity_stdout_pq(options, test_name, message, flow_id=None):
    if options.teamcity:
        if not flow_id:
            flow_id = threading.current_thread().native_id
        print("##teamcity[testStdOut name='%s' flowId='%s' out='%s']" % (test_name, flow_id, teamcity_escape(message)))


def test_teamcity_endtest(options, test_name):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        # teamcity_endtest(test_name)
        print("##teamcity[testFinished name='%s']" % test_name, flush=True)


def test_teamcity_endtest_pq(options, test_name, parent_flow_id):
    """ Wrapper over teamcity function to only execute the call when in teamcity """
    if options.teamcity:
        flow_id = threading.current_thread().native_id
        print("##teamcity[testFinished name='%s' flowId='%s']" % (test_name, flow_id))
        print("##teamcity[flowFinished name='%s' flowId='%s' parent='%s']" % (test_name, flow_id, parent_flow_id))


def test_teamcity_failtest(options, test_name, message):
    """ Wrapper over teamcity function to only execute the call when in teamcity
    """
    if options.teamcity:
        # teamcity_failtest(test_name, message)
        print("##teamcity[testFailed name='%s' message='%s']" % (test_name, teamcity_escape(message)), flush=True)


def test_teamcity_failtest_pq(options, test_name, message, flow_id=None):
    """ Wrapper over teamcity function to only execute the call when in teamcity
    """
    if options.teamcity:
        if not flow_id:
            flow_id = threading.current_thread().native_id
        print("##teamcity[testFailed name='%s' flowId='%s' message='%s']" % (test_name, flow_id, teamcity_escape(message)))


def text_in_events(messages, message_token):
    return bool(message_token in messages.get_events())


def text_in_log(search_text, search_from_text='') -> bool:
    """ Will search for text in the test logfile starting from the start of the
        story in the log or the top of the file if search_from_text is blank.
    """
    return bool(get_line_from_log(search_text, search_from_text=search_from_text) is not None)


def text_in_messages(messages, log_text) -> bool:
    return bool([_ for _ in messages.get_messages() if log_text in _])


def text_in_warnings(messages, log_text) -> bool:
    return bool([_ for _ in messages.get_warnings() if log_text in _])
