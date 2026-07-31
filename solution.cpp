#ifndef __PROGTEST__
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <climits>
#include <cfloat>
#include <cassert>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <vector>
#include <set>
#include <list>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <stack>
#include <deque>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <compare>
#include <condition_variable>
#include <span>
#include <optional>
#include "progtest_solver.h"
#include "sample_tester.h"
#endif /* __PROGTEST__ */

class CSentinelHacker
{
  public:
    static bool                        seqSolve                                ( const std::vector<uint64_t>         & fragments,
                                                                                 CBigInt                             & res ){
        // Temporary local bus
        auto serializer = createMsgSerializer();

        bool isValid = false;
        CBigInt maxAns = 0;

        // Found callback
        auto foundCb = [&](uint32_t id, const uint8_t* bitfield, size_t bitLen) {
            CBigInt currentAns = countExpressions(bitfield, bitLen);
            if (!isValid || currentAns > maxAns) {
                maxAns = currentAns;
                isValid = true;
            }
        };

        auto finishedCb = [&](uint32_t id) {
            // Nothing to do here - we already have the answer in maxAns
        };

        serializer->addProblem(fragments, foundCb, finishedCb);
        serializer->solve();

        if (isValid) {
            res = maxAns;
            return true;
        }

        return false; // Fragments were corrupted and foundCb was never called
    };
    void                               addTransmitter                          ( ATransmitter                          x ){
        transmitters.push_back(x);
    };
    void                               addReceiver                             ( AReceiver                             x ){
        receivers.push_back(x);
    };
    void                               addFragment                             ( uint64_t                              x ){
        processSingleFragment(x);
    };
    void                               start                                   ( unsigned                              thrCount ){
        isStopped = false;
        workersDone = false;
        busHasPassengers = false;
        // Create the very first empty "bus" before any threads start
        currentSerializer = createMsgSerializer();

        for (auto& trans : transmitters)
            transmitterThreads.emplace_back(&CSentinelHacker::transmitterLoop, this, trans);

        for (unsigned i = 0; i < thrCount; ++i)
            workerThreads.emplace_back(&CSentinelHacker::workerLoop, this);

        for (auto& recv : receivers)
            receiverThreads.emplace_back(&CSentinelHacker::receiverLoop, this, recv);

    };
    void                               stop                                    (){
        // Wait for Receivers
        for (auto& t : receiverThreads)
            if (t.joinable()) t.join();

        {
            std::unique_lock<std::mutex> lock(mtx);

            //  Any fragments left in the sorter now are orphaned/incomplete
            for (const auto& pair : fragmentSorter) {
                completedResultsQueue.push({static_cast<uint32_t>(pair.first), 0, false});
            }
            fragmentSorter.clear();

            //  Only push if we actually added problems to it
            if (currentSerializer && busHasPassengers) {
                jobQueue.push({currentSerializer, currentSerializer->totalThreads()});
                currentSerializer = nullptr;
            }

            isStopped = true;
        }

        cv_workers.notify_all();

        // Wait for Workers
        for (auto& t : workerThreads)
            if (t.joinable()) t.join();

        {
            std::unique_lock<std::mutex> lock(mtx);
            workersDone = true;
        }
        cv_transmitters.notify_all();

        for (auto& t : transmitterThreads)
            if (t.joinable()) t.join();

        receiverThreads.clear();
        workerThreads.clear();
        transmitterThreads.clear();
        receivers.clear();
        transmitters.clear();
    };

  private:
    std::vector<std::thread> receiverThreads;
    std::vector<std::thread> workerThreads;
    std::vector<std::thread> transmitterThreads;

    std::vector<AReceiver> receivers;
    std::vector<ATransmitter> transmitters;

    void receiverLoop(AReceiver recv);
    void workerLoop();
    void transmitterLoop(ATransmitter trans);

    std::map<uint64_t, std::vector<uint64_t>> fragmentSorter;
    std::queue<std::vector<uint64_t>> readyMessagesQueue;

    std::mutex mtx;
    std::condition_variable cv_workers;
    std::condition_variable cv_transmitters;

    bool isStopped = false;
    bool workersDone = false;
    bool busHasPassengers = false;

    std::shared_ptr<CMsgSerializer> currentSerializer;

    struct SolveJob {
        std::shared_ptr<CMsgSerializer> serializer;
        unsigned int threadsRemaining;
    };

    std::queue<SolveJob> jobQueue;
    struct MsgContext {
        uint64_t id;
        CBigInt maxAns = 0;
        bool isValid = false;
        std::mutex ctxMtx; // Protects maxAns and isValid
    };
    // Struct to hold the final answer before transmission
    struct FinalResult {
        uint32_t msgID;
        CBigInt count;
        bool isValid; // true if we found an answer, false if it was corrupted/incomplete
    };

    // Queue where workers drop off the final answers
    std::queue<FinalResult> completedResultsQueue;

    void processSingleFragment(uint64_t fragmentData);
};

void CSentinelHacker::receiverLoop(AReceiver recv) {
    uint64_t fragmentData;
    while (recv->recv(fragmentData))
        processSingleFragment(fragmentData);

}

void CSentinelHacker::workerLoop() {
    while (true) {
        std::shared_ptr<CMsgSerializer> myJob;

        {
            std::unique_lock<std::mutex> lock(mtx);

            // Go to sleep if there are no jobs, wake up if a job arrives or we are stopping
            cv_workers.wait(lock, [this]() {
                return !jobQueue.empty() || isStopped;
            });

            // If we are told to stop and the queue is totally empty - kill the thread
            if (isStopped && jobQueue.empty())
                return;

            myJob = jobQueue.front().serializer;

            jobQueue.front().threadsRemaining--;

            if (jobQueue.front().threadsRemaining == 0)
                jobQueue.pop();

        }
        myJob->solve();
    }
}

void CSentinelHacker::processSingleFragment(uint64_t fragmentData) {
    std::unique_lock<std::mutex> lock(mtx);

    uint64_t msgID = fragmentData >> SHIFT_MSG_ID;

    // Put fragment in the sorter
    fragmentSorter[msgID].push_back(fragmentData);

    // Calculate expected count based on the VERY FIRST fragment we received for this ID
    uint32_t expectedCnt = ((fragmentSorter[msgID][0] >> SHIFT_FRAGMENT_CNT) & MASK_FRAGMENT_CNT) + 1;

    if (fragmentSorter[msgID].size() == expectedCnt) {

        // Consistency check
        bool isConsistent = true;
        for (uint64_t f : fragmentSorter[msgID]) {
            if (((f >> SHIFT_FRAGMENT_CNT) & MASK_FRAGMENT_CNT) + 1 != expectedCnt) {
                isConsistent = false;
                break;
            }
        }

        if (!isConsistent) {
            // Push an invalid result directly to the transmitters
            completedResultsQueue.push({static_cast<uint32_t>(msgID), 0, false});
            cv_transmitters.notify_one();
        }
        else {

            auto ctx = std::make_shared<MsgContext>();
            ctx->id = msgID;

            auto foundCb = [ctx](uint32_t id, const uint8_t* bitfield, size_t bitLen) {
                CBigInt currentAns = countExpressions(bitfield, bitLen);
                std::unique_lock<std::mutex> lock(ctx->ctxMtx);
                if (!ctx->isValid || currentAns > ctx->maxAns) {
                    ctx->maxAns = currentAns;
                    ctx->isValid = true;
                }
            };

            auto finishedCb = [this, ctx](uint32_t id) {
                {
                    std::unique_lock<std::mutex> lock(this->mtx);
                    this->completedResultsQueue.push({
                                                             static_cast<uint32_t>(ctx->id),
                                                             ctx->maxAns,
                                                             ctx->isValid
                                                     });
                }
                this->cv_transmitters.notify_one();
            };

            currentSerializer->addProblem(fragmentSorter[msgID], foundCb, finishedCb);
            busHasPassengers = true; // We successfully loaded a passenger!

            if (!currentSerializer->hasFreeCapacity()) {
                jobQueue.push({currentSerializer, currentSerializer->totalThreads()});
                currentSerializer = createMsgSerializer();
                busHasPassengers = false; // New bus is empty
                cv_workers.notify_all();
            }
        }

        // Clean up regardless of whether it was valid or corrupted
        fragmentSorter.erase(msgID);
    }
}

void CSentinelHacker::transmitterLoop(ATransmitter trans) {
    while (true) {
        FinalResult resultToSend;

        {
            std::unique_lock<std::mutex> lock(mtx);

            // Sleep until there is a result ready or we are told to stop
            cv_transmitters.wait(lock, [this]() {
                return !completedResultsQueue.empty() || workersDone;
            });

            // If we are stopped and the queue is completely empty - we can safely exit
            if (workersDone && completedResultsQueue.empty())
                return;

            resultToSend = completedResultsQueue.front();
            completedResultsQueue.pop();
        }

        if (resultToSend.isValid)
            trans->send(resultToSend.msgID, resultToSend.count);
        else
            trans->incomplete(resultToSend.msgID);

    }
}
// TODO: CSentinelHacker implementation goes here
//-------------------------------------------------------------------------------------------------------------------------------------------------------------
#ifndef __PROGTEST__
int                                    main                                    ()
{
  using namespace std::placeholders;

  msgSerializerLimits ( 100, 1, 1, 1, 1 ); // friendly CMsgSerializer instances in sequential tests
  for ( const auto & x : g_TestSets )
  {
    CBigInt res;
    assert ( CSentinelHacker::seqSolve ( x . m_Fragments, res ) );
    assert ( CBigInt ( x . m_Result ) == res );
  }
  msgSerializerLimits ( 4, 3, 5, 2, 2 ); // more exciting boundaries for real runs

  CSentinelHacker test;
  auto            trans = std::make_shared<CExampleTransmitter> ();
  AReceiver       recv  = std::make_shared<CExampleReceiver> ( std::initializer_list<uint64_t> { 0x508e000072ba, 0x508a000004a1, 0x788a0000058c, 0x246700000092 } );

  test . addTransmitter ( trans );
  test . addReceiver ( recv );
  test . start ( 3 );

  static std::initializer_list<uint64_t> t1Data = { 0x247300061fa2, 0x246d00003977, 0x5c8e000029aa, 0x5c890000009b };
  std::thread t1 ( fragmentSender, std::bind ( &CSentinelHacker::addFragment, &test, _1 ), t1Data );

  static std::initializer_list<uint64_t> t2Data = { 0x788d000036c6, 0x788e00002ab0, 0x508a0000036c, 0x246b00000e2b };
  std::thread t2 ( fragmentSender, bind ( &CSentinelHacker::addFragment, &test, _1 ), t2Data );
  fragmentSender ( std::bind ( &CSentinelHacker::addFragment, &test, _1 ), std::initializer_list<uint64_t> { 0x508d0000007f, 0x5c8b00000aab, 0x788e00007d7d, 0x508d00002f0b, 0x7893000e6648, 0x5c8f00009f2d } );
  t1 . join ();
  t2 . join ();
  test . stop ();
  assert ( trans -> totalSent () == 2 );
  assert ( trans -> totalIncomplete () == 2 );
  return EXIT_SUCCESS;
}
#endif /* __PROGTEST__ */
