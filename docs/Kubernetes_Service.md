# Running in the Red Hat OpenShift Cluster on IBM Cloud

## Prerequisites
To build and run the sample, you must have the done the following:

* Obtain this repository's contents, either use `git` or just download the samples as a ZIP
* Install the [IBM Cloud CLI](https://cloud.ibm.com/docs/cli?topic=cloud-cli-install-ibmcloud-cli)
* Install the [OpenShift Origin (oc) and Kubernetes (kubectl) CLIs](https://cloud.ibm.com/docs/openshift?topic=openshift-openshift-cli)
* Provision an [Event Streams Service Instance](https://cloud.ibm.com/catalog/services/event-streams) in [IBM Cloud®](https://cloud.ibm.com/)
* Provision a [Red Hat OpenShift Cluster Instance](https://cloud.ibm.com/kubernetes/catalog/openshiftcluster) in [IBM Cloud®](https://cloud.ibm.com/). See this [tutorial](https://cloud.ibm.com/docs/openshift?topic=openshift-getting-started#openshift_gs_cluster) to create a Red Hat OpenShift cluster on IBM Cloud.


## Deploy the Application

1. [Access the OpenShift Web Console](https://cloud.ibm.com/docs/openshift?topic=openshift-getting-started#openshift_gs_cluster) of the OpenShift cluster you have created in the prerequisites. Paste the `oc login` command copied from the OpenShift Web Console into your terminal to authenticate via the CLI.

2. Using the CLI, create a project for your Kafka Node.js applications. For example:
    ```shell
    oc new-project k8s-mvp
    ```
    The command above creates a project in your cluster named `k8s-mvp`.

3. From the Event Streams for IBM Cloud instance dashboard, click `Service Credentials` and select or create a new one. Copy its content. 

4. To deploy the application you first need to bind the Event Streams for IBM Cloud service instance to the cluster. Replace `<Service Credentials>` with the content copied in step 3.
    ```shell
    oc create secret generic eventstreams-binding --from-literal=binding='<Service Credentials>'
    ```
    The command above creates a secret in your cluster named `eventstreams-binding`. 

5. From the OpenShift Web Console, select the `Service Catalog` dashboard and click on the `MongoDB` service.

    ![alt text](RHOS-Service-Catalog.png "Red Hat OpenShift Service Catalog")

6. Follow the screen flow to create the MongoDB database instance.

    * In the Information panel, click `Next`.

    ![alt text](MongoDB-1-Info.png "MongDB Info Panel")

    * In the Configuration panel, select the Project created in step 2 for `Add to Project` and provide values for `MongoDB Connection Username`, `MongoDB Connection Password` and `MongoDB Admin Password`. You can leave the other fields in their respective default values. Then, click `Next`.

    ![alt text](MongoDB-2-Config1.png "MongDB Configuration Panel Part 1")

    ![alt text](MongoDB-2-Config2.png "MongDB Configuration Panel Part 1")

    * Finally, click `Create` to create the MongoDB database service.

    ![alt text](MongoDB-3-Binding.png "MongDB Binding Panel")

7. In the terminal, use the CLI to create a kafka-producer application. For example:
    ```shell
    oc new-app https://github.com/kkkum/kafka-nodejs-service --name kafka-producer -e TYPE=producer
    ```
    The command above creates a message producing application in your project named `kafka-producer`.

8. In the terminal, use the CLI to create a kafka-consumer application that stores messages received into the MongoDB database which you created in step 6. For example:
    ```shell
    oc new-app https://github.com/kkkum/kafka-nodejs-service --name kafka-consumer -e TYPE=consumer MONGO_URL=mongodb://mongouser:mongopassword@mongodb.k8s-mvp.svc.cluster.local/sampledb CONSUMER_CLIENT_ID=kafka-consumer1 CONSUMER_GROUP_ID=kafka-consumer-group1
    ```
    * The command above creates a message consuming application in your project named `kafka-consumer`.
    * Its CONSUMER_CLIENT_ID is `kafka-consumer1` and CONSUMER_GROUP_ID is `kafka-consumer-group1`.
    * In the example MONGO_URL, `monguser` is the Connection Username, `mongopassword` is the Connection Password, `mongodb.k8s-mvp.svc.cluster.local` is the Cluster Hostname of the mongodb service and `sampledb` is the Database Name. Ensure that the values match the configuration of the MongoDB instance you created in step 6.

9. For each application deployment e.g. kafka-consumer and kafka-producer, set the environment variable VCAP_SERVICES to the value of the key `binding` of the secret `eventstreams-binding` created in step 4.

    * In the OpenShift Web Console, select the Application Console.

    ![alt text](RHOS-1-App-Console.png "RHOS Select Application Console")

    * Select the Project e.g. k8s-mvp

    ![alt text](RHOS-2-Project.png "RHOS Select Project")

    * Select the Deployment Configuration of the Application e.g. kafka-consumer or kafka-producer

    ![alt text](RHOS-3-Deploy-Config.png "RHOS Select Deployment Configuration")

    * In Deployments screen of the selected Application e.g. kafka-consumer, click the `Environment` tab

    ![alt text](RHOS-4-Environment.png "RHOS Configure Environment")

    * Click `Add Value from Config Map or Secret`
    * Add the environment variable with the name `VCAP_SERVICES`, select the resource `eventstreams-binding` and the key `binding`.

    ![alt text](RHOS-5-Environment.png "RHOS Add Environment Variable VCAP_SERVICES")

    * Click the `Save` button.

10. Access the application logs using the CLI.

    * Use the `oc get pods` command to get the list of application pods:
    ```shell
    oc get po
    ```
    
    * Then use the `oc logs` command to view the logs from a specific application pod in the list. For example:
    ```shell
    oc logs kafka-consumer-1-qklcn --follow
    ```
    The command above follows the logs from the application pod named `kafka-consumer-1-qklcn`.

## Further references

If you want find out more about Red Hat OpenShift on IBM Cloud or Kubernetes then check the following documents:

[Red Hat OpenShift on IBM Cloud](https://www.ibm.com/cloud/openshift)

[Kubernetes Documentation](https://kubernetes.io/docs/home/)


