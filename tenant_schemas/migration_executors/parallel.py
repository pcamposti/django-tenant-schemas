import os
import functools
import multiprocessing

from django.conf import settings

from tenant_schemas.migration_executors.base import MigrationExecutor, run_migrations


class ParallelExecutor(MigrationExecutor):
    codename = 'parallel'

    def run_tenant_migrations(self, tenants):
        db = self.options.get('db', None) or self.options.get('database', None)
        os.system(f"echo {db}")
        if tenants:
            count = 0
            for tenant in tenants:
                os.system(f"echo {tenant}")
                count += 1
                os.system(f"echo python manage.py migrate_schemas tenant --database={db} --schema={tenant}")
                os.system(f'python manage.py migrate_schemas tenant --database={db} --schema={tenant} &')
                os.system(f"echo python manage.py migrate_schemas commons_pg --database={db} --schema={tenant}")
                os.system(f'python manage.py migrate_schemas commons_pg --database={db} --schema={tenant} &')
                if count == 5:
                    count = 0
                    os.system(f"echo wait {str(count)}")
                    os.system('wait')
            os.system('wait')
