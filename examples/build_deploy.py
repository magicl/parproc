#!/usr/bin/env python
# pylint: disable=unused-argument,duplicate-code,fixme,invalid-name

import argparse
import time

import parproc as pp  # pylint: disable=import-error


def define_procs() -> None:
    """Define all processes."""

    # QQQQ:
    # - Can we type the parameters to the proto?

    # TODO:
    # - Both jobs and procs can depend on @ handle.. proto
    # - Allow jobs to skip if something is already done.. And pass down that they were skipped!!

    @pp.Proto(name='cdn.upload')
    def upload(context: pp.ProcContext) -> None:
        time.sleep(1)

    @pp.Proto(name='k8s.build::[clusterName]')
    def build(context: pp.ProcContext, clusterName: str) -> None:
        time.sleep(1)

    @pp.Proto(name='k8s.push::[clusterName]', deps=['@k8s.build::[clusterName]'])
    def push(context: pp.ProcContext, clusterName: str) -> None:
        time.sleep(1)

    @pp.Proto(name='k8s.deploy::[clusterName]', deps=['@k8s.push::[clusterName]', '@cdn.upload'])
    def deploy(context: pp.ProcContext, clusterName: str) -> None:
        time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description='Build and deploy processes')
    parser.add_argument(
        '--no-live', action='store_false', dest='live', default=True, help='Disable live/dynamic terminal output'
    )
    parser.add_argument('cluster_name', type=str, default='prod')
    args = parser.parse_args()

    pp.set_options(dynamic=args.live, task_db_path='.output/parproc.db')
    define_procs()

    # Run the deploy task, which will automatically create and run all dependencies
    pp.run('k8s.deploy::[clusterName]', clusterName=args.cluster_name)

    try:
        pp.wait_for_all()
    except:  # pylint: disable=bare-except # nosec try_except_pass
        pass


if __name__ == '__main__':
    main()
