#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.TestKit
open Akka.FSharp
open System.Collections.Generic


//Creating the system ------->
let WholeSystem = ActorSystem.Create("FSharp")


//Taking input from the command lines ------->
//Argument 1 - Total Number of nodes ------->
let totNodes = int fsi.CommandLineArgs.[1]
//Argument 2 - Topology to be used ------->
let topology = fsi.CommandLineArgs.[2]
//Argument 3 - Algorithm to be used ------->
let algo = fsi.CommandLineArgs.[3]
//Argument 4 - Number of Nodes to be killed ------->
let terminateXnodes = int fsi.CommandLineArgs.[4]

//To count the number of nodes converged------->
let mutable globalCounter: int = 0
//The list that stored terminated nodes------->
let terminatedNodes = new List<int>()

//The list that keeps the s/r ratio for every node ------->
let rsRatio = new List<float>()
//Initializing the node ratio for every node to zero------>
for i in 0 .. totNodes - 1 do
    rsRatio.Add(float 0)


//Defining the types of messages for supervisor actor-------->
type receivedSuperMessage =
    | StartGossipSupervisor of int * string * string
    | DoneGossipSupervisor of string
    | EndPushSumSupervisor of string * int * float


//Defining the types of messages for worker actors-------->
type receivedWorkerMessage = 
    | CallGossipWorkerNode of int * string
    | EndGossipWorkerNode of string * int
    | CallPushSumWorkerNode of int * float * float
    | EndPushSumWorkerNode of string * int * float * float


//The map that stores neighbors corresponding each actor -------->
let Structure = new Dictionary<int, List<int>>()


//Selecting the random neighbor of an actor------->
let randNumGenerator (length:int) : int = 
    let r = System.Random()
    let num = r.Next(length)
    num


//Selecting a random node to start the rumor with-------->
let rand = System.Random()
let mutable randNode = rand.Next()
//The flag that checks if all nodes have finished gossiping----->
let mutable endFlag = false


//Function to check the previous and current s/r ratio --------->
let checkRsRatio (prevRatio : float) (currentRatio : float) : bool = 
    let bool_val : bool = Math.Abs(prevRatio - currentRatio) <= float 0.0000000001
    bool_val


//The worker function for each actor denoted by a node------>
let Node id (mailbox: Actor<_>) =
    //
    let rec nodeLoop (nodeCount: int) (pushsumFlag: bool) (nodeSum: float) (nodeWeight: float) (endCounter: int) (prevRatio: float) () =
        actor {
            //Receive message from the actor's inbox------->
            let! receivedWorkerMessage = mailbox.Receive()

            //d update the variables with the received values for PushSum------->
            //Stores updated sum---->
            let mutable sumAfterUpdate = nodeSum
            //Stores updated weight---->
            let mutable weightAfterUpdate = nodeWeight
            //Stores updated flag---->
            let mutable flagAfterUpdate = pushsumFlag
            //Stores updated counter---->
            let mutable counterAfterUpdate = endCounter
            //Stores updated r to s ratio---->
            let mutable prevRsRatioAfterUpdate = prevRatio

            //Make the actor do work by matching the type of message------->
            match receivedWorkerMessage with

            //Continue spreading Gossip through node-------->
            | CallGossipWorkerNode (index, message) ->
                //If node has received the gossip less than 10 times------->
                if nodeCount < 10 then
                    //If the node has received gossip 9 times
                    //d spread the gossip------>
                    if nodeCount = 9 then
                        printfn "Node finished: %d" id
                        WholeSystem.ActorSelection("user/Supervisor")
                        <! DoneGossipSupervisor("Done")

                    //Select a random neighbor from the list----->
                    let length = Structure.Item(index).Count
                    let randNode = randNumGenerator(length)
                    let neighbourIndex = Structure.Item(index).Item(randNode) // Get the exact index of the node based on list index

                    WholeSystem.ActorSelection(String.Concat("user/Node", neighbourIndex))
                    <! CallGossipWorkerNode(neighbourIndex, message)
                    WholeSystem.ActorSelection(String.Concat("user/Node", index))
                    <! CallGossipWorkerNode(index, message)
                //If node has received the gossip 10 times------->
                //End the current node-------->
                else
                    WholeSystem.ActorSelection(String.Concat("user/Node", index))
                    <! EndGossipWorkerNode("Done", index)


            //End the Gossiping Node-------->
            | EndGossipWorkerNode (someString, index) ->
                let length = Structure.Item(index).Count
                let randNode = randNumGenerator(length)
                let neighbourIndex = Structure.Item(index).Item(randNode)
                WholeSystem.ActorSelection(String.Concat("user/Node", neighbourIndex))
                <! CallGossipWorkerNode(neighbourIndex, "Gossip")


            //Continue calculating/spreading push Sum-------->
            | CallPushSumWorkerNode (index, sum, weight) ->
                //
                if flagAfterUpdate then
                    sumAfterUpdate <- sumAfterUpdate + sum
                    weightAfterUpdate <- weightAfterUpdate + weight
                    let currentRatio = sumAfterUpdate / weightAfterUpdate
                    sumAfterUpdate <- sumAfterUpdate / float 2
                    weightAfterUpdate <- weightAfterUpdate / float 2
                    
                    //Calculate the r to s ratio------>
                    let ratioLow : bool = checkRsRatio prevRsRatioAfterUpdate currentRatio 
                    //If the node doesn't qualify the r to s ratio rule------>
                    if ratioLow then
                        if counterAfterUpdate = 2 then
                            flagAfterUpdate <- false
                            printfn "Node finished: %d" index
                            //Finish the current node------>
                            WholeSystem.ActorSelection("user/Supervisor")
                            <! EndPushSumSupervisor("Done", index, currentRatio)
                        else
                            counterAfterUpdate <- counterAfterUpdate + 1
                    //Else set the counter back to 0-------->
                    else
                        counterAfterUpdate <- 0
                    prevRsRatioAfterUpdate <- currentRatio

                    //Select a random neighbor from the list and push sum------->
                    let length = Structure.Item(index).Count
                    let randNode = rand.Next(length)
                    let neighbourIndex = Structure.Item(index).Item(randNode)

                    //Push the sum------->
                    WholeSystem.ActorSelection("user/Node" + string neighbourIndex)
                    <! CallPushSumWorkerNode(neighbourIndex, sumAfterUpdate, weightAfterUpdate)
                    WholeSystem.ActorSelection("user/Node" + string index)
                    <! CallPushSumWorkerNode(index, sumAfterUpdate, weightAfterUpdate)
                else
                    //Finish the current node------->
                    WholeSystem.ActorSelection(String.Concat("user/Node", index))
                    <! EndPushSumWorkerNode("Done", index, sum, weight)

            //
            | EndPushSumWorkerNode (someString, index1, sum1, weight1) ->
                let length = Structure.Item(index1).Count
                randNode <- randNumGenerator(length)
                let neighbour = Structure.Item(index1).Item(randNode)
                WholeSystem.ActorSelection(String.Concat("user/Node", neighbour))
                <! CallPushSumWorkerNode(neighbour, sum1, weight1)

            //| _ -> failwith "fail from node"

            return! nodeLoop (nodeCount + 1) flagAfterUpdate sumAfterUpdate weightAfterUpdate counterAfterUpdate prevRsRatioAfterUpdate ()
        }

    nodeLoop 0 true (float id) (float 1) 0 (float 1000) ()


//function that creates a 3D structure -------->
let create3D (totNodes: int) =
    let cube_side: int = int (MathF.Cbrt(float32 totNodes))
    let side_sq : int = pown cube_side 2
    for pos = 0 to totNodes - 1 do
        let neighborList = new List<int>()
        //Setting neighbor value in a list--------->
        if ((pos - 1) >= 0) then neighborList.Add(pos - 1)
        if ((pos - cube_side) >= 0) then neighborList.Add(pos - cube_side)
        if ((pos - side_sq) >= 0) then neighborList.Add(pos - side_sq)
        if ((pos + 1) < totNodes) then neighborList.Add(pos + 1)
        if ((pos + cube_side) < totNodes) then neighborList.Add(pos + cube_side)
        if ((pos + side_sq) < totNodes) then neighborList.Add(pos + side_sq)
        Structure.Add(pos, neighborList)
()


//function that creates an Imperfect 3D structure -------->
let createImperfect3D (totNodes: int) =
    let cube_side: int = int (MathF.Cbrt(float32 totNodes))
    let side_sq : int = pown cube_side 2
    for pos = 0 to totNodes - 1 do
        let neighborList = new List<int>()
        //Setting neighbor value in a list--------->
        if ((pos - 1) >= 0) then neighborList.Add(pos - 1)
        if ((pos - cube_side) >= 0) then neighborList.Add(pos - cube_side)
        if ((pos - side_sq) >= 0) then neighborList.Add(pos - side_sq)
        if ((pos + 1) < totNodes) then neighborList.Add(pos + 1)
        if ((pos + cube_side) < totNodes) then neighborList.Add(pos + cube_side)
        if ((pos + side_sq) < totNodes) then neighborList.Add(pos + side_sq)
                        
        //Selecting that random neighbor using random number selection-------->
        let mutable randNode = randNumGenerator(totNodes - 1)
        while neighborList.Contains(randNode) && randNode <> pos do
            randNode <- randNumGenerator(totNodes - 1)
        Structure.Add(pos, neighborList)
()


//function that creates a full network -------->
let createFull (totNodes: int) =
    for pos = 0 to totNodes - 1 do
        //create a new neighbor list for every node------->
        let neighborList = new List<int>()
        //loop through all the neighbors and add them to the list------->
        for other = 0 to totNodes - 1 do
            if (pos <> other) then neighborList.Add(other)
        Structure.Add(pos, neighborList)
()


//function that creates a linear network -------->
let createLinear (totNodes: int) =
    for pos = 0 to totNodes - 1 do
        //If it is an ending node then add previous neighbor to neighbor list------->
        if (pos = totNodes - 1) then
            let neighborList = new List<int>()
            neighborList.Add(pos - 1)
            Structure.Add(pos, neighborList)
        //If it is a starting node then add next neighbor to neighbor list------->
        elif (pos = 0) then
            let neighborList = new List<int>()
            neighborList.Add(pos + 1)
            Structure.Add(pos, neighborList)
        //If it is a middle node then add previous and next neighbor to neighbor list------->
        else
            let neighborList = new List<int>()
            neighborList.Add(pos - 1)
            neighborList.Add(pos + 1)
            Structure.Add(pos, neighborList)
()


//Function that calls the topology by matching the input in the command line------->
let callTopology (topology : String) = 
    printfn "Algorithm - %s \nTopology - %s \nNumber of Nodes - %i" algo topology totNodes
    match topology with
    | "3D" -> create3D totNodes
    | "Imperfect3D" -> createImperfect3D totNodes
    | "Line" -> createLinear totNodes
    | "FullNetwork" -> createFull totNodes
                    //
    | _ -> failwith "Topology doesn't exist."


//Function that checks the input algorithm and implements the correct one-------->
let callAlgo (algo : String) = 
    let mutable res : bool = true
    match algo with
    | "Gossip" ->
        res <- true
    | "Push sum" ->
        res <- false
    | _ -> res <- true
    res


//The worker 
let Supervisor =
    spawn WholeSystem "Supervisor"
    <| fun mailbox ->
        let rec SupervisorLoop counter () =
            actor {
                let! receivedSuperMessage = mailbox.Receive()
                match receivedSuperMessage with
                //Match received message with 
                | StartGossipSupervisor (totNodes, topology, algo) ->
                    callTopology topology

                    let NodesList =
                        //Creating a list for all the nodes there are in the system-------->
                        [ 0 .. totNodes - 1 ]
                        |> List.map (fun id -> spawn WholeSystem ("Node" + string id) (Node id))

                    let mutable stoppingFlag = false
                    //loop through terminateXnodes and pick random nodes to kill
                    for i = 0 to terminateXnodes do
                        randNode <- rand.Next(totNodes - 1)
                        stoppingFlag <- false
                        //If the selected number already doesn't exist in terminated nodes list
                        //Then add the number to the terminated nodes list
                        while not (terminatedNodes.Contains(randNode)) && not stoppingFlag do
                            terminatedNodes.Add(randNode)
                            NodesList.Item(randNode).Tell(PoisonPill.Instance)
                            stoppingFlag <- true
                    
                    
                    //generate a random node to start the algorithm with------>
                    randNode <- rand.Next(totNodes - 1)
                    //See if it doesn't exist in the terminated nodes list------->
                    while terminatedNodes.Contains(randNode) do
                        randNode <- rand.Next(totNodes - 1)


                    let res : bool = callAlgo algo
                    if res = true then 
                        
                        //Start gossiping after selecting the random node------>
                        NodesList.Item(randNode) <! CallGossipWorkerNode(randNode, "Some Gossip ---->")
                    else
                        NodesList.Item(randNode)
                        //Start push sum algorithm------->
                        <! CallPushSumWorkerNode(randNode, float 0, float 1)

                //Check if all the nodes have done gossiping-------->
                | DoneGossipSupervisor (someMsg) ->
                    globalCounter <- globalCounter + 1
                    if counter = totNodes - 1 then
                        printfn "All nodes converged. \n ------Exiting----------"
                        //set the end flag to true------->
                        endFlag <- true

                //End the Push Sum when done with the work-------->
                | EndPushSumSupervisor ("Done", index, ratio) ->
                    rsRatio.Item(index) <- ratio
                    globalCounter <- globalCounter + 1
                    if counter = totNodes then
                        printfn "Worker Super Done"
                        //set the end flag to true------->
                        endFlag <- true

                | _ -> failwith "unknown message"

                return! SupervisorLoop (counter + 1) ()

            }

        SupervisorLoop 0 ()

let startTime = DateTime.Now
let timer = Diagnostics.Stopwatch.StartNew()

Supervisor
<! StartGossipSupervisor(totNodes, topology, algo)

//if end flag is still false,
//then continue with the process--------->
while not endFlag  && (((DateTime.Now - startTime)).TotalMinutes < 1.0) do
    ignore ()

timer.Stop()
printfn "Number of nodes converged: %d" globalCounter
printfn "Time taken by %s for Convergence: %f milliseconds" algo timer.Elapsed.TotalMilliseconds

