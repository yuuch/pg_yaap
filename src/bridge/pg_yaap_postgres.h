#ifndef PG_YAAP_POSTGRES_H
#define PG_YAAP_POSTGRES_H

/*
 * Include PostgreSQL server headers inside an extern "C" block when
 * compiling as C++, then undef common gettext macros to avoid conflicts
 * with system <libintl.h> pulled in by C++ standard headers.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#ifdef __cplusplus
}
#endif

#ifdef gettext
#undef gettext
#endif
#ifdef dgettext
#undef dgettext
#endif
#ifdef dngettext
#undef dngettext
#endif
#ifdef ngettext
#undef ngettext
#endif
#ifdef textdomain
#undef textdomain
#endif

#endif /* PG_YAAP_POSTGRES_H */
