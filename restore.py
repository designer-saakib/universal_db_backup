import sys
import yaml
import subprocess
from pathlib import Path

CONFIG_FILE = "config.yml"

with open(CONFIG_FILE) as f:
    config = yaml.safe_load(f)

def find_instance(dbtype, name):
    for inst in config[dbtype]["instances"]:
        if inst["name"] == name:
            return inst
    raise ValueError(f"{dbtype} instance '{name}' not found")


def ensure_mysql_db(inst, dbname):
    image = inst.get("image", "mysql:8")

    cmd = [
        "docker", "run", "--rm",
        image,
        "mysql",
        "-h", inst["host"],
        "-P", str(inst.get("port", 3306)),
        "-u", inst["user"],
        f"-p{inst['password']}",
        "-e", f"CREATE DATABASE IF NOT EXISTS `{dbname}`;"
    ]

    print(f"Ensuring MySQL database exists: {dbname}")
    subprocess.run(cmd, check=True)


def ensure_postgres_db(inst, dbname):
    image = inst.get("image", "postgres:16")

    cmd = [
        "docker", "run", "--rm",
        "-e", f"PGPASSWORD={inst['password']}",
        image,
        "psql",
        "-h", inst["host"],
        "-p", str(inst.get("port", 5432)),
        "-U", inst["user"],
        "-tc",
        f"SELECT 1 FROM pg_database WHERE datname='{dbname}';"
    ]

    exists = subprocess.check_output(cmd, text=True).strip()

    if not exists:
        print(f"Creating PostgreSQL database: {dbname}")
        create_cmd = [
            "docker", "run", "--rm",
            "-e", f"PGPASSWORD={inst['password']}",
            image,
            "createdb",
            "-h", inst["host"],
            "-p", str(inst.get("port", 5432)),
            "-U", inst["user"],
            dbname
        ]
        subprocess.run(create_cmd, check=True)

def restore_mysql(inst, backup_file, mode, source_db=None, target_db=None):
    image = inst.get("image", "mysql:8")

    if mode != "single":
        raise RuntimeError("MySQL restore only supports single-db backups")

    if not source_db or not target_db:
        raise ValueError("single mode requires source_db and target_db")

    ensure_mysql_db(inst, target_db)

    mysql_cmd = [
        "docker", "run", "-i", "--rm",
        image,
        "mysql",
        "-h", inst["host"],
        "-P", str(inst.get("port", 3306)),
        "-u", inst["user"],
        f"-p{inst['password']}",
        target_db
    ]

    print("Running MySQL restore:")
    print(" ", " ".join(mysql_cmd))

    with open(backup_file, "rb") as f:
        gunzip = subprocess.Popen(
            ["gunzip", "-c"],
            stdin=f,
            stdout=subprocess.PIPE
        )
        subprocess.run(mysql_cmd, stdin=gunzip.stdout, check=True)

    print("MySQL restore complete")

def restore_postgresql(inst, backup_file, mode, source_db=None, target_db=None):
    image = inst.get("image", "postgres:16")

    if mode == "single":
        if not source_db or not target_db:
            raise ValueError("single mode requires source_db and target_db")

        ensure_postgres_db(inst, target_db)

        cmd = [
            "docker", "run", "-i", "--rm",
            "-e", f"PGPASSWORD={inst['password']}",
            image,
            "pg_restore",
            "-h", inst["host"],
            "-p", str(inst.get("port", 5432)),
            "-U", inst["user"],
            "-d", target_db,
            "--clean",
            "--if-exists"
        ]

        print("Running PostgreSQL single-db restore:")
        print(" ", " ".join(cmd))

        with open(backup_file, "rb") as f:
            subprocess.run(cmd, stdin=f, check=True)

    elif mode == "all":
        cmd = [
            "docker", "run", "-i", "--rm",
            "-e", f"PGPASSWORD={inst['password']}",
            image,
            "psql",
            "-h", inst["host"],
            "-p", str(inst.get("port", 5432)),
            "-U", inst["user"]
        ]

        print("Running PostgreSQL full restore:")
        print(" ", " ".join(cmd))

        with open(backup_file, "rb") as f:
            subprocess.run(cmd, stdin=f, check=True)

    else:
        raise ValueError("mode must be 'single' or 'all'")

    print("PostgreSQL restore complete")

def main():
    if len(sys.argv) < 6:
        print(
            "Usage:\n"
            "  restore.py mysql <instance> <backup_file> single <source_db> <target_db>\n"
            "  restore.py postgresql <instance> <backup_file> single <source_db> <target_db>\n"
            "  restore.py postgresql <instance> <backup_file> all - -"
        )
        sys.exit(1)

    dbtype = sys.argv[1]
    inst_name = sys.argv[2]
    backup_file = Path(sys.argv[3])
    mode = sys.argv[4]
    source_db = sys.argv[5] if sys.argv[5] != "-" else None
    target_db = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != "-" else None

    inst = find_instance(dbtype, inst_name)

    if dbtype == "mysql":
        restore_mysql(inst, backup_file, mode, source_db, target_db)
    elif dbtype == "postgresql":
        restore_postgresql(inst, backup_file, mode, source_db, target_db)
    else:
        raise ValueError("Unsupported database type")


if __name__ == "__main__":
    main()
