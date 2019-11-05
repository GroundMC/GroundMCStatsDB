pipeline {
  agent any
  tools {
        maven 'Maven3'
        jdk 'Java11'
  }
  options {
    buildDiscarder logRotator(numToKeepStr: '10')
  }
  stages {
    stage('Clean') {
      steps {
        sh 'mvn clean'
      }
    }
    stage('Compile') {
      steps {
        sh 'mvn compile'
      }
    }
    stage('Test') {
      steps {
        sh 'mvn test'
      }
    }
    stage('Package') {
      steps {
        sh 'mvn package'
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'target/*.jar', excludes: 'target/original*'
      junit testResults: 'target/surefire-reports/*.xml', allowEmptyResults: true
      withCredentials([string(credentialsId: 'ground-discord-webhook-url', variable: 'url')]) {
        discordSend description: 'New plugin build\n@here', footer: 'Update', link: BUILD_URL, successful: currentBuild.resultIsBetterOrEqualTo('SUCCESS'), title: JOB_NAME, webhookURL: url
      }
    }
  }
}
