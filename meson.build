project(
  'pg_yaap',
  ['c', 'cpp'],
  version: '1.0',
  default_options: [
    'cpp_std=c++17',
    'warning_level=1',
  ],
)

fs = import('fs')

pg_config = find_program(get_option('pg_config'), required: true)
grep = find_program('grep', required: true)

pg_includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
pg_pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
pg_sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
pg_bindir = run_command(pg_config, '--bindir', check: true).stdout().strip()

pg_cppflags_raw = run_command(pg_config, '--cppflags', check: true).stdout().strip()
pg_cppflags = []
if pg_cppflags_raw != ''
  pg_cppflags = pg_cppflags_raw.split()
endif

pg_cflags_raw = run_command(pg_config, '--cflags', check: true).stdout().strip()
pg_cflags = []
if pg_cflags_raw != ''
  pg_cflags = pg_cflags_raw.split()
endif

pg_cflags_sl_raw = run_command(pg_config, '--cflags_sl', check: true).stdout().strip()
pg_cflags_sl = []
if pg_cflags_sl_raw != ''
  pg_cflags_sl = pg_cflags_sl_raw.split()
endif

pg_ldflags_raw = run_command(pg_config, '--ldflags', check: true).stdout().strip()
pg_ldflags = []
if pg_ldflags_raw != ''
  pg_ldflags = pg_ldflags_raw.split()
endif

pg_ldflags_sl_raw = run_command(pg_config, '--ldflags_sl', check: true).stdout().strip()
pg_ldflags_sl = []
if pg_ldflags_sl_raw != ''
  pg_ldflags_sl = pg_ldflags_sl_raw.split()
endif

pg_libs_raw = run_command(pg_config, '--libs', check: true).stdout().strip()
pg_libs = []
if pg_libs_raw != ''
  pg_libs = pg_libs_raw.split()
endif

pg_config_header = join_paths(pg_includedir_server, 'pg_config.h')
if not fs.exists(pg_config_header)
  error('Could not find pg_config.h under ' + pg_includedir_server)
endif

pg_has_llvm = run_command(grep, '-q', 'USE_LLVM 1', pg_config_header, check: false).returncode() == 0

llvm_cppflags = []
llvm_ldflags = []
llvm_libs = []
if pg_has_llvm
  llvm_config_opt = get_option('llvm_config')
  if llvm_config_opt == ''
    llvm_config = find_program(['llvm-config', 'llvm-config-19', 'llvm-config-18'], required: true)
  else
    llvm_config = find_program(llvm_config_opt, required: true)
  endif

  llvm_cppflags_raw = run_command(llvm_config, '--cflags', check: true).stdout().strip()
  if llvm_cppflags_raw != ''
    llvm_cppflags = llvm_cppflags_raw.split()
  endif

  llvm_ldflags_raw = run_command(llvm_config, '--ldflags', check: true).stdout().strip()
  if llvm_ldflags_raw != ''
    llvm_ldflags = llvm_ldflags_raw.split()
  endif

  llvm_libs_raw = run_command(llvm_config, '--libs', '--system-libs', 'core', 'orcjit', 'native', check: true).stdout().strip()
  if llvm_libs_raw != ''
    llvm_libs = llvm_libs_raw.split()
  endif
endif

message('Using pg_config: ' + pg_config.full_path())
message('PostgreSQL include dir: ' + pg_includedir_server)
message('PostgreSQL module install dir: ' + pg_pkglibdir)
message('PostgreSQL shared dir: ' + pg_sharedir)
message('PostgreSQL binary dir: ' + pg_bindir)
if pg_has_llvm
  message('PostgreSQL built with LLVM: yes')
else
  message('PostgreSQL built with LLVM: no')
endif

pg_yaap_sources = []

pg_yaap_optimize_args = [
  '-O3',
  '-march=native',
  '-ftree-vectorize',
  '-funroll-loops',
  '-ffast-math',
]

pg_yaap_inc = include_directories(
  'src/bridge',
  'src/executor/engine',
  'src/optimizer',
)

subdir('src/bridge')
subdir('src/executor')
subdir('src/optimizer')

visibility_args = ['-fvisibility=hidden']
common_cpp_args = pg_cppflags + pg_cflags + pg_cflags_sl + visibility_args + ['-fvisibility-inlines-hidden'] + pg_yaap_optimize_args + ['-I' + pg_includedir_server]
# Force-include a header that includes postgres.h and undefines gettext macros
common_cpp_args += ['-include', 'pg_yaap_postgres.h']
common_c_args = pg_cppflags + pg_cflags + pg_cflags_sl + visibility_args + pg_yaap_optimize_args + ['-I' + pg_includedir_server]

if pg_has_llvm
  common_cpp_args += llvm_cppflags
  common_c_args += llvm_cppflags
endif

pg_yaap_link_args = pg_ldflags + pg_ldflags_sl + pg_libs + llvm_ldflags + llvm_libs
if host_machine.system() == 'darwin'
  pg_yaap_link_args += ['-bundle_loader', join_paths(pg_bindir, 'postgres')]
endif

pg_yaap = shared_module(
  'pg_yaap',
  pg_yaap_sources,
  include_directories: pg_yaap_inc,
  c_args: common_c_args,
  cpp_args: common_cpp_args,
  link_args: pg_yaap_link_args,
  name_prefix: '',
  install: true,
  install_dir: pg_pkglibdir,
)

install_data(
  [
    'pg_yaap.control',
    'pg_yaap--1.0.sql',
  ],
  install_dir: join_paths(pg_sharedir, 'extension'),
)
