import os
import subprocess
import functools
import multiprocessing

from django.conf import settings

from tenant_schemas.migration_executors.base import MigrationExecutor, run_migrations


class ParallelExecutor(MigrationExecutor):
    codename = 'parallel'

    def work(db, tenant, app):
        command = ['python', 'manage.py', 'migrate_schemas', f'{app}', f'--database={db}', f'--schema={tenant}']
        migrate_tenant = subprocess.Popen(command)

    def run_tenant_migrations(self, tenants):
        db = self.options.get('db', None) or self.options.get('database', None)
        chunks = getattr(settings, 'TENANT_PARALLEL_MIGRATION_CHUNKS', 20)
        print(self)
        print(self.args)
        os.system(f"echo {db}")
        if tenants:
            count = 0
            command = ''
            for tenant in tenants:
                os.system(f"echo {tenant}")
                count += 1
                process = multiprocessing.Process(target=work, args=(db, tenant, 'tenant'))
                process2 = multiprocessing.Process(target=work, args=(db, tenant, 'commons_pg'))
                process.start()
                process2.start()
                os.system(f"echo python manage.py migrate_schemas tenant --database={db} --schema={tenant} ")
                os.system(f"echo python manage.py migrate_schemas commons_pg --database={db} --schema={tenant}")
                if count == chunks:
                    process.join()
                    process2.join()
                    count = 0
            process.join()
            process2.join()
            os.system(f"echo finish Migrations")
