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

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <pthread.h>
#include <fstream>
#include <sstream>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "helper.h"
#include "route_guide.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using grpc::ClientAsyncReader;
using grpc::CompletionQueue;
using routeguide::Point;
using routeguide::Feature;
using routeguide::Rectangle;
using routeguide::RouteSummary;
using routeguide::RouteNote;
using routeguide::RouteGuide;
using routeguide::RouteGuideNew;

Point MakePoint(long latitude, long longitude) {
  Point p;
  p.set_latitude(latitude);
  p.set_longitude(longitude);
  return p;
}

Feature MakeFeature(const std::string& name,
                    long latitude, long longitude) {
  Feature f;
  f.set_name(name);
  f.mutable_location()->CopyFrom(MakePoint(latitude, longitude));
  return f;
}

RouteNote MakeRouteNote(const std::string& message,
                        long latitude, long longitude) {
  RouteNote n;
  n.set_message(message);
  n.mutable_location()->CopyFrom(MakePoint(latitude, longitude));
  return n;
}

class RouteGuideClient {
 public:
  RouteGuideClient(std::shared_ptr<Channel> channel, const std::string& db)
      : stub_(RouteGuide::NewStub(channel)), stubnew_(RouteGuideNew::NewStub(channel)) {
    routeguide::ParseDb(db, &feature_list_);
  }

  /*void GetFeature() {
    Point point;
    Feature feature;
    point = MakePoint(409146138, -746188906);
    GetOneFeature(point, &feature);
    point = MakePoint(0, 0);
    GetOneFeature(point, &feature);
  }*/

  void ListFeatures() {
    routeguide::Rectangle rect1;
    routeguide::Rectangle rect2;
    Feature feature;
    ClientContext context1;
    ClientContext context2;
    CompletionQueue cq;
    Status status;
    
    //rect.mutable_lo()->set_latitude(400000000);
    //rect.mutable_lo()->set_longitude(-750000000);
   // rect.mutable_hi()->set_latitude(420000000);
   // rect.mutable_hi()->set_longitude(-730000000);
   // std::cout << "Looking for features between 40, -75 and 42, -73"
     //         << std::endl;

    std::unique_ptr<ClientAsyncReader<Feature> > reader1(
        stub_->AsyncListFeatures(&context1, rect1, &cq, (void*)1));

    std::unique_ptr<ClientAsyncReader<Feature> > reader2(
        stubnew_->AsyncListFeaturesNew(&context2, rect2, &cq, (void*)2));

    //reader1->Finish(&status, (void*)1);
    

    void* got_tag;
    bool ok = false;

    //cq.Next(&got_tag, &ok);
    
    while (1) {
      cq.Next(&got_tag, &ok);
      if (ok && got_tag == (void*)1)
      {
        reader1->Read(&feature, (void*)1);
        std::cout << "Found feature called : 1"
                << feature.name() << " at "
                << feature.location().latitude()/kCoordFactor_ << ", "
                << feature.location().longitude()/kCoordFactor_ << std::endl;
     }
     else if (ok && got_tag == (void*)2)
     {
        reader2->Read(&feature, (void*)2);
        std::cout << "Found feature called : 2"
                << feature.name() << " at "
                << feature.location().latitude()/kCoordFactor_ << ", "
                << feature.location().longitude()/kCoordFactor_ << std::endl; 
     }
    }

    reader1->Finish(&status, (void*)1);
    reader2->Finish(&status, (void*)2);
    if (status.ok()) {
      std::cout << "ListFeatures rpc succeeded." << std::endl;
    } else {
      std::cout << "ListFeatures rpc failed." << std::endl;
    }
  }

 private:

  const float kCoordFactor_ = 10000000.0;
  std::unique_ptr<RouteGuideNew::Stub> stubnew_;
  std::unique_ptr<RouteGuide::Stub> stub_;
  std::vector<Feature> feature_list_;
};


int main(int argc, char** argv) {

  std::string db = routeguide::GetDbFileContent(argc, argv);

  /*std::ifstream requestfile("/root/cgangwar/certs/cert.pem");
  std::stringstream buffer;
  buffer << requestfile.rdbuf();
  std::string cert = buffer.str();

  grpc::SslCredentialsOptions ssl_opts;
  ssl_opts.pem_root_certs = cert;

*/  
  RouteGuideClient guide(grpc::CreateChannel("10.24.81.171:50051", grpc::InsecureChannelCredentials()), db);
  std::cout << "-------------- ListFeatures --------------" << std::endl;
  guide.ListFeatures();
  
  return 0;
}
