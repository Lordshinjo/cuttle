// @flow

import injectSheet from "react-jss";
import React from "react";
import { connect } from "react-redux";
import Measure from "react-measure";
import _ from "lodash";
import numeral from "numeraljs";
import { navigate } from "redux-url";
import isEqual from "lodash/isEqual";

import ReactPaginate from "react-paginate";
import PrevIcon from "react-icons/lib/md/navigate-before";
import NextIcon from "react-icons/lib/md/navigate-next";
import BreakIcon from "react-icons/lib/md/keyboard-control";
import OpenIcon from "react-icons/lib/md/zoom-in";

import { Badge } from "../../common/components/Badge";
import Spinner from "../../common/components/Spinner";
import Clock from "../../common/components/Clock";
import Table from "../../common/components/Table";
import PopoverMenu from "../../common/components/PopoverMenu";
import { ROW_HEIGHT } from "../../common/components/Table";
import Link from "../../common/components/Link";
import { PostEventSource } from "../../common/Utils";
import type { Paginated } from "../../common/datamodel";
import type { ExecutionLog, Workflow } from "../datamodel";
import Status from "../components/Status";
import Context from "../components/Context";

type Props = {
  classes: any,
  className?: string,
  workflow: Workflow,
  envCritical: boolean,
  request: (
    page: number,
    rowsPerPage: number,
    sort: { column: string, order: "asc" | "desc" }
  ) => any,
  columns: Array<
    | "dag"
    | "job"
    | "context"
    | "startTime"
    | "failed"
    | "retry"
    | "endTime"
    | "status"
    | "detail"
    | "lastFailure"
  >,
  label: string,
  page: number,
  sort: {
    column: string,
    order: "asc" | "desc"
  },
  open: (link: string, replace: boolean) => void,
  selectedJobs: Array<string>,
  completionNotifier?: (?number) => void
};

type State = {
  data: ?Array<ExecutionLog>,
  page: number,
  total: number,
  rowsPerPage: number,
  query: any,
  eventSource: any
};

class ExecutionLogs extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      data: null,
      page: props.page - 1,
      total: -1,
      rowsPerPage: 25,
      query: null,
      eventSource: null
    };
    (this: any).adaptTableHeight = _.throttle(
      this.adaptTableHeight.bind(this),
      1000 // no more than one resize event per second
    );
  }

  componentDidUpdate() {
    let { sort } = this.props;
    let { query, page, rowsPerPage, eventSource } = this.state;
    let newQuery = this.props.request(page, rowsPerPage, sort);
    if (!isEqual(newQuery, query)) {
      eventSource && eventSource.stopPolling();
      eventSource = new PostEventSource(newQuery.endpoint, newQuery);
      eventSource.onmessage(json => {
        this.updateData(json.data);
      });
      eventSource.startPolling();
      this.setState({
        ...this.state,
        query: newQuery,
        data: null,
        eventSource
      });
    }
  }

  componentWillUnmount() {
    let { eventSource } = this.state;
    eventSource && eventSource.stopPolling();
  }

  componentWillReceiveProps(nextProps: Props) {
    this.setState({ ...this.state, page: nextProps.page - 1 });
  }

  adaptTableHeight({ height }) {
    this.setState({
      ...this.state,
      rowsPerPage: Math.max(1, Math.floor(height / ROW_HEIGHT) - 1)
    });
  }

  updateData(json: Paginated<ExecutionLog>) {
    const notify = this.props.completionNotifier;
    if (notify != undefined) {
      notify(json.completion);
    }

    this.setState({
      ...this.state,
      total: json.total,
      page: Math.min(
        this.state.page,
        Math.max(0, Math.ceil(json.total / this.state.rowsPerPage) - 1)
      ),
      data: json.data
    });
  }

  qs(page: number, sort: string, order: "asc" | "desc") {
    return `?page=${page}&sort=${sort}&order=${order}`;
  }

  changePage({ selected }) {
    let { sort } = this.props;
    this.props.open(this.qs(selected + 1, sort.column, sort.order), false);
  }

  sortBy(column: string) {
    let { sort } = this.props;
    let order = "asc";
    if (column == sort.column) {
      order = sort.order == "asc" ? "desc" : "asc";
    }
    this.props.open(this.qs(1, column, order), false);
  }

  render() {
    let { sort, columns, envCritical } = this.props;
    let { data, page, rowsPerPage, total } = this.state;
    let { classes, workflow, label, selectedJobs } = this.props;

    let jobName = (jobId: string) => {
      let job = workflow.getJob(jobId);
      if (job) {
        return job.name;
      } else {
        return jobId;
      }
    };

    let dagName = (jobId: string) => {
      let dag = workflow.getDagFromJob(jobId);
      if (dag) {
        return dag.name;
      } else {
        return "";
      }
    };

    let dataTable = (() => {
      if (data && data.length) {
        return (
          <Table
            envCritical={envCritical}
            columns={columns.map(column => {
              switch (column) {
                case "dag":
                  return { id: "dag", label: "Dag", sortable: true };
                case "job":
                  return { id: "job", label: "Job", sortable: true };
                case "context":
                  return { id: "context", label: "Context", sortable: true };
                case "failed":
                  return { id: "failed", label: "Failed", sortable: true };
                case "retry":
                  return {
                    id: "retry",
                    label: "Next retry",
                    sortable: true,
                    width: 200
                  };
                case "startTime":
                  return { id: "startTime", label: "Started", sortable: true };
                case "endTime":
                  return { id: "endTime", label: "Finished", sortable: true };
                case "status":
                  return {
                    id: "status",
                    label: "Status",
                    width: 120,
                    sortable: true
                  };
                case "detail":
                  return { id: "detail", width: 40 };
                case "lastFailure":
                  return { id: "lastFailure", label: "Last failure" };
              }
            })}
            onSortBy={this.sortBy.bind(this)}
            sort={sort}
            data={data}
            render={(
              column,
              { id, job, startTime, endTime, status, context, failing }
            ) => {
              switch (column) {
                case "dag":
                  return dagName(job);
                case "job":
                  return jobName(job);
                case "context":
                  return <Context context={context} />;
                case "failed": {
                  let times = (failing && failing.failedExecutions.length) || 0;
                  if (times == 1) {
                    return "Once";
                  } else if (times > 1) {
                    return `${times} times`;
                  }
                  return "";
                }
                case "startTime":
                  return (
                    <Clock className={classes.time} time={startTime || ""} />
                  );
                case "endTime":
                  return (
                    <Clock className={classes.time} time={endTime || ""} />
                  );
                case "retry":
                  return status !== "throttled" ? (
                    "Now"
                  ) : (
                    <Clock
                      className={classes.time}
                      time={(failing && failing.nextRetry) || ""}
                    />
                  );
                case "status":
                  return (
                    <Link
                      className={classes.openIcon}
                      href={`/executions/${id}`}
                    >
                      <Status status={status} />
                    </Link>
                  );
                case "detail":
                  return (
                    <Link
                      className={classes.openIcon}
                      href={`/executions/${id}`}
                    >
                      <OpenIcon />
                    </Link>
                  );
                case "lastFailure":
                  const lastFailedUrl = `/executions/${
                    failing.failedExecutions[
                      failing.failedExecutions.length - 1
                    ].id
                  }`;

                  return (
                    <Link href={lastFailedUrl}>
                      <Badge label="FAILED" kind="error" width={75} />
                    </Link>
                  );
              }
            }}
          />
        );
      } else if (data) {
        return (
          <div className={classes.noData}>
            No {label} executions for now
            {selectedJobs.length ? " (some may have been filtered)" : ""}
          </div>
        );
      } else {
        return <Spinner />;
      }
    })();

    let Pagination = () => {
      if (total >= 2 && total <= rowsPerPage) {
        return (
          <div className={classes.footer}>{`${total} ${label} executions`}</div>
        );
      } else if (total > rowsPerPage) {
        let pageCount = Math.ceil(total / rowsPerPage);
        return (
          <div className={classes.footer}>
            {`${numeral(page * rowsPerPage + 1).format("0,0")} to ${numeral(
              Math.min(total, page * rowsPerPage + rowsPerPage)
            ).format("0,0")} of ${numeral(total).format(
              "0,0"
            )} ${label} executions`}
            <ReactPaginate
              pageCount={pageCount}
              pageRangeDisplayed={3}
              marginPagesDisplayed={2}
              forcePage={page}
              previousLabel={<PrevIcon className={classes.paginationIcon} />}
              nextLabel={<NextIcon className={classes.paginationIcon} />}
              breakLabel={<BreakIcon className={classes.paginationIcon} />}
              containerClassName={classes.pagination}
              activeClassName={classes.paginationActive}
              onPageChange={this.changePage.bind(this)}
            />
          </div>
        );
      } else {
        return <div className={classes.footer} />;
      }
    };

    return (
      <div className={classes.grid}>
        <Measure onMeasure={this.adaptTableHeight}>
          <div className={classes.data}>{dataTable}</div>
        </Measure>
        <Pagination />
      </div>
    );
  }
}

const styles = {
  container: {
    padding: "1em",
    flex: "1",
    display: "flex",
    flexDirection: "column",
    position: "relative"
  },
  title: {
    fontSize: "1.2em",
    margin: "0 0 16px 0",
    color: "#607e96",
    fontWeight: "normal"
  },
  menu: {
    position: "absolute",
    top: "1em",
    right: "1em"
  },
  grid: {
    flex: "1",
    display: "flex",
    flexDirection: "column"
  },
  data: {
    display: "flex",
    flex: "1",
    minWidth: "800px"
  },
  noData: {
    flex: "1",
    textAlign: "center",
    fontSize: "0.9em",
    color: "#8089a2",
    alignSelf: "center",
    paddingBottom: "15%"
  },
  time: {
    color: "#85929c"
  },
  openIcon: {
    fontSize: "22px",
    color: "#607e96",
    padding: "15px",
    margin: "-15px"
  },
  footer: {
    display: "flex",
    height: "2em",
    margin: ".8em 0 0 0",
    lineHeight: "2em",
    fontSize: ".9em",
    color: "#8089a2",
    background: "#ecf1f5"
  },
  pagination: {
    margin: "0",
    flex: "1",
    textAlign: "right",
    transform: "translateX(1em)",
    "& li": {
      display: "inline-block"
    },
    "& li a": {
      padding: "10px",
      cursor: "pointer",
      userSelect: "none",
      outline: "none"
    }
  },
  paginationActive: {
    background: "#d6dfe6",
    color: "#4a6880",
    borderRadius: "2px"
  },
  paginationIcon: {
    fontSize: "1.5em",
    transform: "translateY(-1px)"
  }
};

const mapStateToProps = ({
  app: { project, workflow, page, selectedJobs }
}) => ({
  workflow,
  page: page.page || 1,
  sort: page.sort,
  order: page.order,
  selectedJobs: selectedJobs,
  envCritical: project.env.critical
});
const mapDispatchToProps = dispatch => ({
  open(href, replace) {
    dispatch(navigate(href, replace));
  }
});

export const Finished = connect(
  mapStateToProps,
  mapDispatchToProps
)(
  injectSheet(styles)(
    ({
      classes,
      workflow,
      page,
      sort,
      order,
      open,
      selectedJobs,
      envCritical
    }) => {
      return (
        <div className={classes.container}>
          <h1 className={classes.title}>Finished executions</h1>
          <ExecutionLogs
            envCritical={envCritical}
            classes={classes}
            open={open}
            page={page}
            workflow={workflow}
            columns={["dag", "job", "context", "endTime", "status", "detail"]}
            request={(page, rowsPerPage, sort) => {
              return {
                endpoint: "/api/executions/status/finished",
                jobs: selectedJobs,
                sort: sort,
                limit: rowsPerPage,
                offset: page * rowsPerPage
              };
            }}
            label="finished"
            sort={{ column: sort || "endTime", order: order || "desc" }}
            selectedJobs={selectedJobs}
          />
        </div>
      );
    }
  )
);

export const Started = connect(
  mapStateToProps,
  mapDispatchToProps
)(
  injectSheet(styles)(
    ({
      classes,
      workflow,
      page,
      sort,
      order,
      open,
      selectedJobs,
      envCritical
    }) => {
      return (
        <div className={classes.container}>
          <h1 className={classes.title}>Started executions</h1>
          <ExecutionLogs
            envCritical={envCritical}
            classes={classes}
            open={open}
            page={page}
            workflow={workflow}
            columns={["job", "context", "startTime", "status", "detail"]}
            request={(page, rowsPerPage, sort) => {
              return {
                endpoint: "/api/executions/status/started",
                jobs: selectedJobs,
                sort: sort,
                limit: rowsPerPage,
                offset: page * rowsPerPage
              };
            }}
            label="started"
            sort={{ column: sort || "context", order: order || "asc" }}
            selectedJobs={selectedJobs}
          />
        </div>
      );
    }
  )
);

export const Retrying = connect(
  mapStateToProps,
  mapDispatchToProps
)(
  injectSheet(styles)(
    ({
      classes,
      workflow,
      page,
      sort,
      order,
      open,
      selectedJobs,
      envCritical
    }) => {
      const relaunch = () => {
        fetch("/api/executions/relaunch", {
          method: "POST",
          credentials: "include",
          body: JSON.stringify({ jobs: selectedJobs })
        });
      };

      return (
        <div className={classes.container}>
          <h1 className={classes.title}>Retrying executions</h1>
          <PopoverMenu
            className={classes.menu}
            items={[<span onClick={relaunch}>Relaunch everything now</span>]}
          />
          <ExecutionLogs
            envCritical={envCritical}
            classes={classes}
            open={open}
            page={page}
            workflow={workflow}
            columns={[
              "job",
              "context",
              "retry",
              "lastFailure",
              "status",
              "detail"
            ]}
            request={(page, rowsPerPage, sort) => {
              return {
                endpoint: "/api/executions/status/retrying",
                jobs: selectedJobs,
                sort: sort,
                limit: rowsPerPage,
                offset: page * rowsPerPage
              };
            }}
            label="retrying"
            sort={{ column: sort || "failed", order: order || "asc" }}
            selectedJobs={selectedJobs}
          />
        </div>
      );
    }
  )
);
