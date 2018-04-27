node {
    checkout scm
    googlePrjectName = "jenkins-193214"
    appName = "tw-reporter"
    sh "git rev-parse --short HEAD > commit-id"
    tag = readFile('commit-id').replace("\n", "").replace("\r", "")
    registryHost = "eu.gcr.io/${googlePrjectName}/${appName}/"
    def images = [:]
    ['reportloader', 'mongodb'].each {
    	images.put("${it}", "${registryHost}${it}")
    }
    
    stage ('Build') {
    	images.each { appname, imagename ->
    		/* Build image for the application */
        	sh "docker build -t ${imagename}:latest -f ./dockerfiles/${appname} ."
		};
    }
    
    stage ('Tag') {
    	images.each { appname, imagename ->
    		/* Build image for the application */
        	sh "gcloud docker -- tag ${imagename}:latest ${imagename}:${tag}"
		};
	}
	
    stage ('Push') {
    	images.each { appname, imagename ->
    		/* Build image for the application */
        	sh "gcloud docker -- push ${imagename}:latest"
        	sh "gcloud docker -- push ${imagename}:${tag}"
		};
	}
	
    stage ('Deploy-Testing') {
    	/* substitute image version tags*/
    	sed_pattern = "s/IMAGE_VERSION/${tag}/g"
    	sh "sed -i ${sed_pattern} manifests/testing/deployment_testing.yaml"
    	 	    	
		/* Deploy the testing pod */
    	sh "kubectl apply -f manifests/testing/"
    	
    	/* Wait for the deployment to rollout successfully */
    	sh "kubectl rollout status deployments tw-reporter"
    	/* wait for 5 secs to give the time in system to be ready */
    	sleep(10)
    }
    
    stage ('Testing') {
    	    	
		/* Run test for pulling data */
    	sh """
    	   kubectl exec -it `kubectl --namespace=jenkins get pods | \
    	    grep Running | grep tw-reporter |cut -d " " -f 1` -c reportloader  -- \
    	   /bin/bash -c "python3 reportloader/tests/pull_data.py"
    	   """
    	   
    	/* Run test for pushing data */
    	sh """
    	   kubectl exec -it `kubectl --namespace=jenkins get pods | \
    	    grep Running | grep tw-reporter |cut -d " " -f 1` -c reportloader  -- \
    	   /bin/bash -c "python3 reportloader/tests/data_pusher.py"
    	   """   	   
    }
    
    stage ('Deploy') {
    	/* substitute image version tags*/
    	sed_pattern = "s/IMAGE_VERSION/${tag}/g"
    	sh "sed -i ${sed_pattern} manifests/production/deployment_reportloader.yaml"
    	 	  
    	/* set the context to be the project-agora cluster*/
    	sh """
    	    kubectl config set-credentials \
    	    project-agora/gke_jenkins-193214_europe-west1-b_project-agora \
    	    --username=admin --password=eylaZbhYcZQRJAxz
    	   """
    	sh """
    		kubectl config set-cluster \
    		gke_jenkins-193214_europe-west1-b_project-agora \
    		--insecure-skip-tls-verify=true --server=https://35.189.200.199
    	   """
    	sh """
    		kubectl config set-context \
    		tw-reporter/gke_jenkins-193214_europe-west1-b_project-agora/project-agora \
    		--user=project-agora/gke_jenkins-193214_europe-west1-b_project-agora \
    		--namespace=tw-reporter --cluster=gke_jenkins-193214_europe-west1-b_project-agora
    		"""
    	sh """
    		kubectl config use-context \
    		tw-reporter/gke_jenkins-193214_europe-west1-b_project-agora/project-agora
    		"""
    	sh "kubectl apply -f manifests/production/"
    	
    	/* Wait for the deployment to rollout successfully */
    	sh "kubectl rollout status deployments mongodb"
    	sh "kubectl rollout status deployments reportloader"
    }
}