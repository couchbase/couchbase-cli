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
    agent { label "linux&&master" }

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

                dir("${WORKSPACE}") {    
                    timeout(time: 10, unit: "MINUTES") {
                        sh """#!/bin/bash
                            python3 -m venv env
                            source env/bin/activate

                            # Install the tools required to lint and fetch coverage profiles
                            pip3 install pylint==2.10.2 mypy==1.3.0 coverage==5.2 pytest==5.4.3 autopep8==1.5.7 types-pyOpenSSL==23.2.0.0 types-requests==2.31.0.1

                            # Install the dependencies required to build/run the cli tools
                            pip3 install cryptography==3.4.8 pem==21.2.0 pycryptodome==3.10.1 pyopenssl==20.0.1 python-snappy==0.6.0 requests==2.26.0 requests-toolbelt==0.9.1
                        """
                    }
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

                        sh """#!/bin/bash
                            source ${WORKSPACE}/env/bin/activate
                            CXXFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" CFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" CPPFLAGS=\"-I${WORKSPACE}/snappy-build/usr/local/include -L${WORKSPACE}/snappy-build/usr/local/lib\" pip3 install python-snappy
                        """
                    }
                }
            }
        }

        stage("Lint") {
            steps {
                timeout(time: 10, unit: "MINUTES") {
                    dir("${PROJECTPATH}") {
                        sh """#!/bin/bash
                            source ${WORKSPACE}/env/bin/activate
                            python3 -m pylint -E --disable=import-error cblogredaction cbrecovery cbworkloadgen couchbase-cli pump*.py
                            python3 -m pylint --disable=import-error,unused-import --disable C,R cbmgr.py cluster_manager.py
                            python3 -m autopep8 --diff --max-line-length=120 --experimental --exit-code -aaa \$(find -name '*.py')
                        """
                    }
                }
            }
        }

        stage("Spell check docs") {
            steps {
                dir("${PROJECTPATH}") {
                    sh "./jenkins/adoc-lint.sh"
                }
            }
        }

        stage("Type check") {
            steps {
                timeout(time: 10, unit: "MINUTES") {
                    dir("${PROJECTPATH}") {
                        sh """#!/bin/bash
                            source ${WORKSPACE}/env/bin/activate
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
                // Make reports directory if it does not exist
                sh "mkdir -p ${WORKSPACE}/reports"

                dir("${PROJECTPATH}"){
                    sh """#!/bin/bash
                        source ${WORKSPACE}/env/bin/activate

                        # Use pytest to run the test as it is nicer and also can produce junit xml reports
                        coverage run --source . -m pytest test/test_*.py --cache-clear --junitxml=${WORKSPACE}/reports/test-cli.xml -v

                        # Produce xml report for cobertura
                        coverage xml -o ${WORKSPACE}/reports/coverage-cli.xml
                    """
                }
             }
         }
    }

    post {
         always {
            // Post the test results
            junit allowEmptyResults: true, testResults: "reports/test-*.xml"

            // Post the test coverage
            cobertura autoUpdateStability: false, autoUpdateHealth: false, onlyStable: false, coberturaReportFile: "reports/coverage-*.xml", conditionalCoverageTargets: "70, 10, 30", failNoReports: false, failUnhealthy: true, failUnstable: true, lineCoverageTargets: "70, 10, 30", methodCoverageTargets: "70, 10, 30", maxNumberOfBuilds: 0, sourceEncoding: "ASCII", zoomCoverageChart: false
        }

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
