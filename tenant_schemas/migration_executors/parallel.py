import functools
import multiprocessing

from django.conf import settings

from tenant_schemas.migration_executors.base import MigrationExecutor, run_migrations


class ParallelExecutor(MigrationExecutor):
    codename = 'parallel'

    def run_tenant_migrations(self, tenants):
        if tenants:
            tenants = tenants.split(',') if ',' in tenants else tenants
            print(tenants)
            processes = getattr(settings, 'TENANT_PARALLEL_MIGRATION_MAX_PROCESSES', 4)
            chunks = getattr(settings, 'TENANT_PARALLEL_MIGRATION_CHUNKS', 2)

            from django.db import connection

            run_migrations_p = functools.partial(
                run_migrations,
                self.args,
                self.options,
                self.codename,
                allow_atomic=False
            )
            p = multiprocessing.Pool(processes=processes)
            p.map(run_migrations_p, tenants)