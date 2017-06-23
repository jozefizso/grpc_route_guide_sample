/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <map>
#include <vector>
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>
#include "helper.h"
#include "route_guide.grpc.pb.h"
#include <pthread.h>
#include <fstream>
#include <sstream>
#include <grpc++/impl/service_type.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::ServerAsyncWriter;
using grpc::ServerCompletionQueue;
using grpc::Status;
using routeguide::Point;
using routeguide::Feature;
using routeguide::Rectangle;
using routeguide::RouteSummary;
using routeguide::RouteNote;
using routeguide::RouteGuide;
using routeguide::RouteGuideNew;
using std::chrono::system_clock;
using grpc::AsynchronousService;


class OperationalObject
{
    public: 
    OperationalObject (){}
    
};

class GrpcClientBase
{
    protected:
        GrpcClientBase (){}
    public:    
     virtual ~GrpcClientBase () {}

     virtual void   postToClient () = 0;
     virtual bool   isAlive () = 0;
     virtual void   formData (OperationalObject object) = 0;
};

template<class T> class GrpcClient : public GrpcClientBase
{
    public:
        GrpcClient (ServerAsyncWriter<T>* writer, void *tag, ServerContext *context)
        {
            m_writer = writer;
            m_tag = tag;
            m_context = context;
        }

        ~GrpcClient ()
        {
            std::cout << "deleting GrpcClient" << std::endl;
            m_writer = NULL;
            m_context = NULL;
        }
     
        void postToClient ()
        {
                m_writer->Write (m_data, m_tag);    
        }

        bool isAlive ()
        {
            return (!m_context->IsCancelled ());            
        }
    
        void formData (OperationalObject object)
        {
            std::cout << "Updating data" << std::endl;
            //call some function to fill protobuf data from operational object.
        }

    private:
    ServerAsyncWriter<T>*       m_writer;   
    void*                       m_tag; 
    ServerContext*              m_context;
    T                           m_data;

};

class PrismMutex
{
    public:
    PrismMutex ()
    {
        pthread_mutex_init (&m_mutex, NULL);
    }

    void lock ()
    {
        pthread_mutex_lock (&m_mutex);
    }

    void unlock ()
    {
        pthread_mutex_unlock (&m_mutex);
    }

    private :
        pthread_mutex_t m_mutex;

};

//This  will part of Grpc Notification Thread OM
class GrpcClientToolkit
{
    public:
        GrpcClientToolkit ()
        {
        }
    
        static void addClient (std::string name, GrpcClientBase *client)
        {
            m_mutex.lock();
            m_grpcClientMap[name].insert (client);
            m_mutex.unlock();
        }

        static std::map<std::string, std::set<GrpcClientBase*> > m_grpcClientMap;
        static PrismMutex                                        m_mutex;
};

std::map<std::string, std::set<GrpcClientBase*> > GrpcClientToolkit::m_grpcClientMap;
PrismMutex                                        GrpcClientToolkit::m_mutex;

class CallDataBase
{
    public:
    CallDataBase ()
    {
    }
    
    virtual void Proceed () = 0;
};

class RouteGuideImpl final  {
 public:
  explicit RouteGuideImpl(const std::string& db) {
    routeguide::ParseDb(db, &feature_list_);
  }

  private:
  // Class encompasing the state and logic needed to serve a request.
  class CallData : public CallDataBase {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallData(RouteGuide::AsyncService* service, ServerCompletionQueue* cq)
        :  service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      count = 1;
      Proceed();
    }

    void Proceed() {
      if (status_ == CREATE) {
        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestListFeatures(&ctx_, &request_, &responder_, cq_, cq_,
                                  this);

        // Make this instance progress to the PROCESS state.
        status_ = PROCESS;
      } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        //new CallData(service_, cq_);

        // The actual processing.
        if (count == 1)
        {
            GrpcClient<Feature> *pClient = new GrpcClient<Feature> (&responder_, this, &ctx_);
            GrpcClientToolkit::addClient("ListFeature", pClient) ;
            new CallData(service_, cq_);
            count++;
        }   


        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
      } else {
        responder_.Finish(Status::OK, this);
        status_ = FINISH;
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    RouteGuide::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    Rectangle request_;
    // What we send back to the client.
    Feature reply_;

    // The means to get back to the client.
    ServerAsyncWriter<Feature> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.

    int count;
  };

  private:
  // Class encompasing the state and logic needed to serve a request.
 class CallDataNew : public CallDataBase{
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    CallDataNew (RouteGuideNew::AsyncService* service, ServerCompletionQueue* cq)
        :  service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
      // Invoke the serving logic right away.
      count = 1;
      Proceed();
    }

    void Proceed() {
      if (status_ == CREATE) {
        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestListFeaturesNew (&ctx_, &request_, &responder_, cq_, cq_,
                                  this);
        std::cout << "CREATE" << std::endl;
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS;
      } else if (status_ == PROCESS) {
      std::cout << "PROCESS" << std::endl;
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        //new CallData(service_, cq_);

        // The actual processing.
        if (count == 1)
        {
            GrpcClient<Feature> *pClient = new GrpcClient<Feature> (&responder_, this, &ctx_);
            GrpcClientToolkit::addClient("ListFeature", pClient) ;
            new CallDataNew (service_, cq_);
            count++;
        }


        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
      } else {
        responder_.Finish(Status::OK, this);
        status_ = FINISH;
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

    private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    RouteGuideNew::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    Rectangle request_;
    // What we send back to the client.
    Feature reply_;

    // The means to get back to the client.
    ServerAsyncWriter<Feature> responder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.

    int count;
  };  
    
public:
  void Run() {
    std::string server_address("0.0.0.0:50051");

 /*   std::ifstream parserequestfile("/root/cgangwar/certs/key.pem");
   std::stringstream buffer;
   buffer << parserequestfile.rdbuf();
   std::string key = buffer.str();

   std::ifstream requestfile("/root/cgangwar/certs/cert.pem");
   buffer << requestfile.rdbuf();
   std::string cert = buffer.str();

  grpc::SslServerCredentialsOptions::PemKeyCertPair pkcp = {key, cert};

  grpc::SslServerCredentialsOptions ssl_opts;
  ssl_opts.pem_root_certs = "";
  ssl_opts.pem_key_cert_pairs.push_back(pkcp);
*/

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterAsyncService(&service_);
    builder.RegisterAsyncService(&servicenew_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    HandleRpcs();
  }

  void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq_.get());
    new CallDataNew (&servicenew_, cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;

    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      cq_->Next(&tag, &ok);
      GPR_ASSERT(ok);
      static_cast<CallDataBase*>(tag)->Proceed();
    }
  }  

 private:

  std::vector<Feature> feature_list_;

  std::unique_ptr<ServerCompletionQueue> cq_;
  RouteGuide::AsyncService service_;
  RouteGuideNew::AsyncService servicenew_;
  std::unique_ptr<Server> server_;   

};

void *GrpcNotificationObjectManagerThread(void *vargp)
{
    std::cout << "printing from Grpc Notification Thread" << std::endl;
    sleep (10);
    
    std::set<GrpcClientBase*> clients = GrpcClientToolkit::m_grpcClientMap["ListFeature"];

    std::set<GrpcClientBase*>::iterator it;
    std::set<GrpcClientBase*>::iterator end;

    while(1) 
    {
        GrpcClientToolkit::m_mutex.lock();
        clients = GrpcClientToolkit::m_grpcClientMap["ListFeature"];
  
        it = clients.begin();
        end = clients.end();

        while (it != end)
        {
            if (*it)
            {
                OperationalObject object;
                (*it)->formData (object);
            }       

            it++;
        }
        GrpcClientToolkit::m_mutex.unlock();

        sleep(3);
    }

    return NULL;
}

void *PostThread(void *arg)
{
    std::cout << "printing from Post thread" << std::endl;
    sleep (10);

    std::set<GrpcClientBase*> clients = GrpcClientToolkit::m_grpcClientMap["ListFeature"];
    std::set<GrpcClientBase*>::iterator it;
    std::set<GrpcClientBase*>::iterator end;

    while(1)
    {
        GrpcClientToolkit::m_mutex.lock();
        clients = GrpcClientToolkit::m_grpcClientMap["ListFeature"];
        std::cout << clients.size() << std::endl;
        
        it = clients.begin();
        end = clients.end();
        
        while (it != end)
        {
            if (*it)
            {
                if ((*it)->isAlive ())
                {
                    (*it)->postToClient ();
                }
                else
                {
                    std::cout << "cleaning the client" << std::endl;
                    delete (*it);
                    it = clients.erase (it);
                    continue;
                }
            }

            it++;
        }

        GrpcClientToolkit::m_mutex.unlock();
        sleep(1);
    }

}
 
int main(int argc, char** argv) {
  // Expect only arg: --db_path=path/to/route_guide_db.json.
  std::string db = routeguide::GetDbFileContent(argc, argv);

  pthread_t tid;
  pthread_create(&tid, NULL, GrpcNotificationObjectManagerThread, NULL);

  std::cout << " " <<std::endl;


  pthread_t tid2;
  pthread_create(&tid2, NULL, PostThread, NULL);
 
  std::cout << " " <<std::endl;

  //RunServer(db);
  RouteGuideImpl *server = new RouteGuideImpl ("");
  server->Run ();

  return 0;
}
