#!/usr/bin/env groovy

/**
 * When updating this Jenkinsfile, changes will not take effect immediately; they will
 * take effect once the Jenkins multi-branch pipeline picks up the commit. This therefore
 * means that changes made to the Jenkinsfile in a Gerrit review will not have any effect
 * until they are submitted.
 */

 import hudson.model.Result
 import hudson.model.Run
 import jenkins.model.CauseOfInterruption.UserInterruption

pipeline {
    agent { label "ubuntu-1604&&master" }

    environment {
        PROJECTPATH="${WORKSPACE}/couchbase-cli"
        CMAKE_CURRENT_BINARY_DIR="${PROJECTPATH}/install"
        CMAKE_CURRENT_SOURCE_DIR="${PROJECTPATH}"
        PATH="${PATH}:${WORKSPACE}/snappy-build/usr/local/include:/home/couchbase/.local/bin"
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${WORKSPACE}/snappy-build/usr/local/lib"
        DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${WORKSPACE}/snappy-build/usr/local/include"
    }

    stages {
        stage("Setup") {
            steps {
                script {
                    // Configure Gerrit Trigger
                    properties([pipelineTriggers([
                        gerrit(
                            serverName: "review.couchbase.org",
                            gerritProjects: [
                                [
                                    compareType: "PLAIN", disableStrictForbiddenFileVerification: false, pattern: "couchbase-cli",
                                    branches: [[ compareType: "PLAIN", pattern: "master" ]]
                                ],
                             ],
                            triggerOnEvents: [
                                commentAddedContains(commentAddedCommentContains: "reverify"),
                                draftPublished(),
                                patchsetCreated(excludeNoCodeChange: true)
                            ]
                        )
                    ])])
                }

                slackSend(
                    channel: "#tooling-cv",
                    color: "good",
                    message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' started (${env.BUILD_URL})"
                )

                timeout(time: 10, unit: "MINUTES") {
                    sh "pip3 install --user urllib3 pylint requests mypy==0.730"
                }

                // preventively delete the cli path to avoid issues with cloning
                dir("${PROJECTPATH}") {
                    deleteDir()
                }

                dir("${WORKSPACE}") {
                    sh "git clone git@github.com:couchbase/couchbase-cli.git"
                }

                // Fetch the commit we are testing
                dir("${PROJECTPATH}") {
                    sh "git fetch ssh://buildbot@review.couchbase.org:29418/couchbase-cli ${GERRIT_REFSPEC}"
                    sh "git checkout FETCH_HEAD"
                }
            }
        }

        stage("Build Dependencies") {
            steps {
                timeout(time: 10, unit: "MINUTES") {
                    sh "mkdir snappy-build"

                    // paranoid cleanup
                    dir("${WORKSPACE}/snappy") {
                        deleteDir()
                    }

                    dir("${WORKSPACE}") {
                        sh "git clone -b 1.1.7 https://github.com/google/snappy.git"
                    }

                    dir("${WORKSPACE}/snappy/build") {
                        sh "cmake ../ -DBUILD_SHARED_LIBS=ON"
                        sh "DESTDIR=${WORKSPACE}/snappy-build make install"
                        sh "CXXFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" CFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" CPPFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" pip3 install --user python-snappy"
                    }
                }
            }
        }

        stage("Lint") {
            steps {
                timeout(time: 10, unit: "MINUTES") {
                    dir("${PROJECTPATH}") {
                        sh "python3 -m pylint -E --disable=import-error cbbackup cbbackupwrapper cblogredaction cbrecovery cbrestore cbrestorewrapper cbtransfer cbworkloadgen couchbase-cli *.py"
                    }
                }
            }
        }

        stage("Type check") {
            steps {
                timeout(time: 10, unit: "MINUTES") {
                    dir("${PROJECTPATH}") {
                        sh """#!/bin/bash
                            if [ \$(mypy --ignore-missing-imports cbbackup | grep -c error) -gt 1 ]; then
                                echo "Failed mypy type checking in cbbackup"
                                echo "Re running: mypy --ignore-missing-imports cbbackup"
                                echo \$(mypy --ignore-missing-imports cbbackup)
                                exit 1
                            fi
                           """

                        sh """#!/bin/bash
                            if [ \$(mypy --ignore-missing-imports cbrestore | grep -c error) -gt 1 ]; then
                                echo "Failed mypy type checking in cbrestore"
                                echo "Re running: mypy --ignore-missing-imports cbrestore"
                                echo \$(mypy --ignore-missing-imports cbrestore)
                                exit 1
                            fi
                           """

                        sh """#!/bin/bash
                            if [ \$(mypy --ignore-missing-imports cbtransfer | grep -c error) -gt 1 ]; then
                                echo "Failed mypy type checking in cbtransfer"
                                echo "Re running: mypy --ignore-missing-imports cbtransfer"
                                echo \$(mypy --ignore-missing-imports cbtransfer)
                                exit 1
                            fi
                           """

                        sh """#!/bin/bash
                            if [ \$(mypy --ignore-missing-imports cbworkloadgen | grep -c error) -gt 1 ]; then
                                echo "Failed mypy type checking in cbworkloadgen"
                                echo "Re running: mypy --ignore-missing-imports cbworkloadgen"
                                echo \$(mypy --ignore-missing-imports cbworkloadgen)
                                exit 1
                            fi
                            """
                    }
                }
            }
        }

        stage("Test") {
            steps {
                dir("${PROJECTPATH}"){
                    echo "----------------------------------------------------------------"
                    echo "Unit test CLI"
                    echo "----------------------------------------------------------------"
                    sh "python3 -m unittest discover -s ./test -p test_cli.py"

                    echo "----------------------------------------------------------------"
                    echo "Unit test pumps"
                    echo "----------------------------------------------------------------"
                    sh "python3 -m unittest discover -s ./test -p test_pumps.py"

                    echo "----------------------------------------------------------------"
                    echo "Unit test TXN"
                    echo "----------------------------------------------------------------"
                    sh "python3 -m unittest discover -s ./test -p test_txn.py"
                }
             }
         }
    }

    post {
        success {
            slackSend(
                channel: "#tooling-cv",
                color: "good",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' succeeded (${env.BUILD_URL})"
            )
        }

        unstable {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' is unstable (${env.BUILD_URL})"
            )
        }

        failure {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' failed (${env.BUILD_URL})"
            )
        }

        aborted {
            slackSend(
                channel: "#tooling-cv",
                color: "bad",
                message: "Build for '<${GERRIT_CHANGE_URL}|${GERRIT_CHANGE_SUBJECT}>' by '${GERRIT_CHANGE_OWNER_NAME}' aborted (${env.BUILD_URL})"
            )
        }

        cleanup {
            // Remove the workspace
            deleteDir()
        }
    }
}
