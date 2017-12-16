pipeline {
  agent any
  tools {
        maven 'Maven3'
        jdk 'Java9'
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
      post {
        success {
          junit 'target/surefire-reports/**/*.xml'
        }
      }
    }
    stage('Package') {
      steps {
        sh 'mvn package'
      }
    }
    stage('Archive') {
      steps {
        archiveArtifacts artifacts: 'target/*.jar', excludes: 'target/original*'
      }
    }
  }
}