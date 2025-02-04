import os
import subprocess
import functools

from django.conf import settings

from tenant_schemas.migration_executors.base import MigrationExecutor, run_migrations


class SchemaExecutor(MigrationExecutor):
    codename = 'schema'

    def run_tenant_migrations(self, tenants):
        db = self.options.get('db', None) or self.options.get('database', None)
        chunks = getattr(settings, 'TENANT_PARALLEL_MIGRATION_CHUNKS', 10)
        app = self.options.get('app_label', None)
        print(f"app {app}: {db}")
        if tenants:
            count = 0
            command = ''
            schemas = ''
            n=4
            tenant_chunks = [tenants[i * n:(i + 1) * n] for i in range((len(tenants) + n - 1) // n )]
            for tenant in tenant_chunks:
                os.system(f"echo {tenant}")
                count += 1
                print(f"python manage.py migrate_schemas {app} --database={db} --schema={tenant} --executor")
                schemas = ",".join(tenant)
                migrate_parallel = subprocess.Popen([
                    'python',
                    'manage.py',
                    'migrate_schemas',
                    f'{app}',
                    f'--database={db}',
                    f'--schema={schemas}',
                    '--executor=parallel',
                ])
                schemas = ''
                if count == chunks:
                    migrate_parallel.wait()
                    count = 0
            migrate_parallel.wait()
            os.system(f"echo finish Migrations")