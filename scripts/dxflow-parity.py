#!/usr/bin/env python3
"""Compare an isolated, synthetic DxFlow MongoDB fixture with local Oxide.

Install scripts/dxflow-parity-requirements.txt in a virtualenv first. This is a
gate, not a migration tool: failed checks produce a nonzero exit status. It never
changes the reference fixture. Only the named candidate fixture and disposable
behavior-test databases are written. Reports contain hashes, not record data.
"""
import argparse
import hashlib
import json
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from bson import Binary, Int64, ObjectId, json_util
from pymongo import IndexModel, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError


def canonical(value):
    return json_util.dumps(value, json_options=json_util.CANONICAL_JSON_OPTIONS,
                           sort_keys=True, separators=(",", ":"))


def digest(collection):
    # Oxide bounds each query snapshot. Large config documents and audit histories
    # must be read in bounded pages rather than asking for one unbounded result.
    batch = 1 if collection.name == "workflows" else 100
    rows = []
    offset = 0
    while True:
        page = list(collection.find({}).sort("_id", 1).skip(offset).limit(batch))
        rows.extend(canonical(row) for row in page)
        if len(page) < batch:
            break
        offset += batch
    rows.sort()
    return {"count": len(rows), "sha256": hashlib.sha256("\n".join(rows).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", default="mongodb://127.0.0.1:27017/?directConnection=true&retryWrites=false")
    parser.add_argument("--candidate", default="mongodb://127.0.0.1:27018/?directConnection=true&retryWrites=false&maxPoolSize=4")
    parser.add_argument("--database", default="hepquant_parity_reference")
    parser.add_argument("--copy", action="store_true", help="Replace candidate fixture collections from reference")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not args.database.startswith("hepquant_parity_"):
        parser.error("fixture database must start with hepquant_parity_")
    for uri in [args.reference, args.candidate]:
        parsed = urlsplit(uri)
        if parsed.scheme != "mongodb" or parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            parser.error("both database endpoints must be single-host loopback MongoDB URIs")
    clients = [MongoClient(uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=40000)
               for uri in [args.reference, args.candidate]]
    reference, candidate = clients
    if "oxideVersion" in reference.admin.command("buildInfo"):
        raise ValueError("reference must be MongoDB")
    info = candidate.admin.command("buildInfo")
    if "oxideVersion" not in info:
        raise ValueError("candidate must be Oxide")
    report = {"oxide_version": info["oxideVersion"], "fixture": args.database, "checks": []}

    def check(name, action):
        try:
            details = action()
            report["checks"].append({"name": name, "passed": True, "details": details})
            print("PASS", name, flush=True)
        except Exception as error:
            # Do not include command bodies: tenant configuration can contain secrets.
            message = str(error).split(", full error:")[0][:500]
            report["checks"].append({"name": name, "passed": False,
                                     "error": type(error).__name__ + ": " + message})
            print("FAIL", name, message, flush=True)

    source, target = reference[args.database], candidate[args.database]
    names = sorted(source.list_collection_names())
    if source.order_data.count_documents({}) < 2:
        raise ValueError("seed the reference lab fixture first")
    if args.copy:
        for name in names:
            def copy(name=name):
                target.drop_collection(name)
                target.create_collection(name)
                batch = []
                for row in source[name].find({}):
                    batch.append(row)
                    if len(batch) == 20:
                        target[name].insert_many(batch)
                        batch = []
                if batch:
                    target[name].insert_many(batch)
                indexes = []
                for spec in source[name].list_indexes():
                    if spec["name"] != "_id_":
                        keys = [(field, int(direction) if direction in (1, -1) else direction)
                                for field, direction in spec["key"].items()]
                        indexes.append(IndexModel(keys, **{key: value for key, value in spec.items()
                                                                  if key not in ("key", "v", "ns")}))
                if indexes:
                    target[name].create_indexes(indexes)
            check("copy/" + name, copy)
    for name in names:
        def compare(name=name):
            expected, actual = digest(source[name]), digest(target[name])
            if expected != actual:
                raise AssertionError(f"BSON digest mismatch: expected={expected}, actual={actual}")
            return expected
        check("data/" + name, compare)
        def indexes(name=name):
            def definitions(collection):
                return sorted([{"name": spec["name"], "key": [(field, int(direction) if direction in (1, -1) else direction)
                                                              for field, direction in spec["key"].items()],
                                "unique": spec.get("unique", spec["name"] == "_id_"),
                                "partial": spec.get("partialFilterExpression")}
                               for spec in collection.list_indexes()], key=lambda spec: spec["name"])
            equal(definitions(source[name]), definitions(target[name]))
        check("indexes/" + name, indexes)
    check("data/collection-set", lambda: equal(names, sorted(target.list_collection_names())))

    # These pipelines were captured from the actual DxFlow MongoDB 2.8 provider
    # stack, rather than reconstructed from a simplified query example.
    for path in sorted((Path(__file__).parent.parent / "tests/fixtures/dxflow").glob("*.json")):
        def replay(path=path):
            pipeline = json_util.loads(path.read_text())
            collection = {"orders": "order_data", "samples": "sample_data", "runs": "run_data"}[path.stem.split("-")[0]]
            expected = sorted(canonical(row) for row in source[collection].aggregate(pipeline))
            actual = sorted(canonical(row) for row in target[collection].aggregate(pipeline))
            equal(expected, actual)
        check("dxflow/" + path.stem, replay)

    # Independent scratch databases make writes repeatable and keep the fixture intact.
    scratch = "oxide_dxflow_behavior_" + uuid.uuid4().hex
    databases = [c[scratch] for c in clients]
    try:
        fixture = [
            {"_id": 1, "value": Int64(1), "tags": ["a", "b"], "foreign_id": 10,
             "date": datetime(2026, 1, 1, tzinfo=timezone.utc), "binary": Binary(b"dxflow", 0),
             "oid": ObjectId("000000000000000000000001")},
            {"_id": 2, "value": Int64(2), "tags": [], "foreign_id": 20},
        ]
        for db in databases:
            db.items.insert_many(fixture)
            db.foreign.insert_many([{"_id": 10, "name": "synthetic A"}, {"_id": 20, "name": "synthetic B"}])

        def paired(action):
            expected, actual = [action(db) for db in databases]
            equal(expected, actual)

        check("behavior/typed-roundtrip", lambda: paired(lambda db: list(db.items.find({}).sort("_id", 1))))
        check("behavior/filter-sort", lambda: paired(lambda db: list(db.items.find({"tags": "a"}).sort("value", -1))))
        pipelines = {
            "lookup": [{"$lookup": {"from": "foreign", "localField": "foreign_id", "foreignField": "_id", "as": "relation"}}],
            "unwind": [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}],
            "add-fields": [{"$addFields": {"double": {"$multiply": ["$value", 2]}}}],
            "unset": [{"$unset": ["tags"]}],
            "replace-root": [{"$project": {"root": {"value": "$value"}}}, {"$replaceRoot": {"newRoot": "$root"}}],
            "map-cond": [{"$project": {"mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$cond": [{"$eq": ["$$tag", "a"]}, "first", "other"]}}}}}],
        }
        for name, pipeline in pipelines.items():
            check("behavior/" + name, lambda pipeline=pipeline: paired(
                lambda db: sorted((canonical(row) for row in db.items.aggregate(pipeline)))))

        def transaction(db, abort):
            with db.client.start_session() as session:
                session.start_transaction()
                db.items.update_one({"_id": 1}, {"$set": {"transaction": "abort" if abort else "commit"}}, session=session)
                inside = db.items.find_one({"_id": 1}, session=session)
                outside = db.items.find_one({"_id": 1})
                if inside == outside:
                    raise AssertionError("uncommitted write leaked or read-your-writes failed")
                if abort:
                    session.abort_transaction()
                    equal(db.items.find_one({"_id": 1}), outside)
                else:
                    session.commit_transaction()
                    equal(db.items.find_one({"_id": 1}), inside)
            return db.items.find_one({"_id": 1})
        for abort in [True, False]:
            check("behavior/transaction-" + ("rollback" if abort else "commit"),
                  lambda abort=abort: paired(lambda db: transaction(db, abort)))

        def versioned(db):
            db.items.update_one({"_id": 2}, {"$set": {"_version": 1}})
            first = db.items.update_one({"_id": 2, "_version": 1}, {"$inc": {"_version": 1}})
            stale = db.items.update_one({"_id": 2, "_version": 1}, {"$inc": {"_version": 1}})
            assert (first.modified_count, stale.modified_count) == (1, 0)
            return db.items.find_one({"_id": 2})
        check("behavior/version-conflict", lambda: paired(versioned))

        def claims(db):
            db.jobs.insert_many([{"_id": n, "queue": "default", "scheduled": n, "payload": Binary(b"synthetic")} for n in range(12)])
            def claim(_):
                return db.jobs.find_one_and_update({"queue": "default"}, {"$set": {"queue": "running"}},
                                                  sort=[("scheduled", 1)], return_document=ReturnDocument.AFTER)["_id"]
            with ThreadPoolExecutor(max_workers=4) as executor:
                ids = sorted(executor.map(claim, range(12)))
            equal(ids, list(range(12)))
            return list(db.jobs.find({}).sort("_id", 1))
        check("behavior/concurrent-job-claims", lambda: paired(claims))
        def tenant_domains(db):
            db.tenants.insert_many([{"_id": 1, "name": "alpha", "domains": ["alpha.test", "shared.test", "shared.test"]},
                                    {"_id": 2, "name": "beta", "domains": ["beta.test"]}])
            db.tenants.create_index("domains", unique=True)
            db.tenants.create_index("name", unique=True)
            try:
                db.tenants.insert_one({"_id": 3, "name": "gamma", "domains": ["shared.test"]})
            except DuplicateKeyError:
                pass
            else:
                raise AssertionError("duplicate tenant domain was accepted")
            try:
                db.tenants.update_one({"_id": 2}, {"$set": {"domains": ["shared.test"]}})
            except DuplicateKeyError:
                pass
            else:
                raise AssertionError("duplicate tenant domain update was accepted")
            equal(db.tenants.find_one({"_id": 2})["domains"], ["beta.test"])
            db.tenants.delete_one({"_id": 1})
            db.tenants.insert_one({"_id": 3, "name": "gamma", "domains": ["shared.test"]})
            return list(db.tenants.find({}).sort("_id", 1))
        check("behavior/tenant-domain-index", lambda: paired(tenant_domains))
        # Real DxFlow keeps tenant settings current through MongoDB change streams.
        def watch(db):
            with db.items.watch(max_await_time_ms=100) as stream:
                db.items.insert_one({"_id": 3})
                event = None
                for _ in range(10):
                    event = stream.try_next()
                    if event:
                        break
                if not event or event["operationType"] != "insert":
                    raise AssertionError("change stream did not deliver the inserted record")
            return True
        check("behavior/change-stream", lambda: paired(watch))
    except Exception as error:
        report["checks"].append({"name": "behavior/setup", "passed": False,
                                 "error": type(error).__name__})
        raise
    finally:
        for db in databases:
            check("cleanup/" + ("reference" if db.client is reference else "candidate"),
                  lambda db=db: db.client.drop_database(db.name))
        for client in clients:
            client.close()
        report["passed"] = all(c["passed"] for c in report["checks"])
        report["cutover_allowed"] = False
        report["remaining_acceptance"] = [
            "DxFlow entity create/update/delete, audit history and rollback through the application API",
            "Real aide-de-camp queue retry and transactional dead-letter behavior",
            "Tenant settings cache invalidation and tenant isolation",
            "Authenticated LIMS/provider-portal smoke test and process-restart persistence",
        ]
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["passed"] else 1


def equal(expected, actual):
    if canonical(expected) != canonical(actual):
        raise AssertionError("reference and candidate outcomes differ")


if __name__ == "__main__":
    sys.exit(main())
