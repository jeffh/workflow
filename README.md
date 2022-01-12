# Workflow

Leveraging state machines as a faster way to build web services. Requires Java 11+

## Problem

Typical web software development needs to handle the following concerns:

 - Implementing new features (hopefully obvious)
 - Reliability of operation & implementation, including upgrades/migrations
 - Observability/Explainability of the system to technical and non-technical people

But today's efforts to address this employ personel & tooling around these
issues, instead of trying to address them directly:

 - Microservices: While a logical way to organize teams, there should be efforts
   to broaden understandability of the system as a whole (for everyone
   involved). Microservices should be easier, faster, & more reliable to build
   that an equivalent monolith.
 - "Micro" Tools: Tools tend to give a limit perspective to solve the wider
   problem. Error tracking, metrics, and log aggregations are a proxy to
   understanding what happened.
 - Processes: Specifications that are independent from code. There is a gap
   understanding how a system work for technical and non-technical people that
   should be narrowed.

Nothing is wrong with those above efforts, but this project attempts to imagine
something different.

In short, the goal is make implementations effortless enough to allow people to
better understand problems to solve instead of how to execute effectively.

## Install

Use [deps.edn](https://clojure.org/reference/deps_and_cli):

```clojure
;; NOTE: replace :git/sha value with the version you want to checkout


;; all-dependencies as one (biggest, but includes everything)
{:deps {net.jeffhui/workflow {:git/url "ssh://git@github.com/jeffh/workflow.git"
                              :git/sha "75d4353c8b46f9748efff20da10090e29bebd774"}}}
                              
;; individual features a-la-carte:
;; - workflow-core = basic library + in-memory implementations
;; - workflow-jdbc-pg = postgres integration for persistence
;; - workflow-kafka = kafka integration for scheduling
;; - workflow-tools-graphviz = for visualizations (requires graphviz to be installed)
;; - workflow-tools-webviewer = for web-based debugging of state machines and executions
{:deps {net.jeffhui/workflow-core {:git/url   "ssh://git@github.com/jeffh/workflow.git"
                                   :git/sha   "..."
                                   :deps/root "workflow-core"}
        net.jeffhui/workflow-jdbc-pg {:git/url   "ssh://git@github.com/jeffh/workflow.git"
                                      :git/sha   "..."
                                      :deps/root "workflow-jdbc-pg"}
        net.jeffhui/workflow-kafka {:git/url   "ssh://git@github.com/jeffh/workflow.git"
                                    :git/sha   "..."
                                    :deps/root "workflow-kafka"}
        net.jeffhui/workflow-tools-graphviz {:git/url   "ssh://git@github.com/jeffh/workflow.git"
                                             :git/sha   "..."
                                             :deps/root "workflow-tools-graphviz"}
        net.jeffhui/workflow-tools-webviewer {:git/url   "ssh://git@github.com/jeffh/workflow.git"
                                              :git/sha   "..."
                                              :deps/root "workflow-tools-webviewer"}}}
```

You can then require them through the corresponding namespaces:

 - (workflow-core) net.jeffhui.workflow.api - for the main API
 - (workflow-core) net.jeffhui.workflow.memory - for using the in-memory implementation
 - (workflow-core) net.jeffhui.workflow.protocol - for implementing your own backing stores (unstable)
 - (workflow-core) net.jeffhui.workflow.contracts - for testing your protocol implements to the spec (unstable)
 - (workflow-jdbc-pg) net.jeffhui.workflow.jdbc.pg - for postgres implementations
 - (workflow-kafka) net.jeffhui.workflow.kafka - for kafka implementations
 - (workflow-tools-graphviz) net.jeffhui.workflow.tools.graphviz - for generating diagrams
 - (workflow-tools-webviewer) net.jeffhui.workflow.tools.webviewer - for viewing state machines + executions on a web ui for diagnostics

## Usage

**Note: Nothing here is a stable API. Things are subject to change at this point in time.**

```clojure
;; TODO
```

## License

Copyright © 2022 Jeff Hui

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.


# Development

Running test:

```bash
# needs services running
cd test-services
docker up -d
cd ..
# run tests
clojure -X:test
```

## Open Research Areas

Here's some extra things to look into:

 - How to maintain high performance, despite more data being generated.
 - How to minimize the amount of need of escape hatches
 - How to share state machine implementations
