# NodeKey is dead!

This project blog is becoming an omage to my favourite playlist of scala heros and tech talks, and so
I guess at this point I should give [Rob Norris's Fixpoint Talk](https://www.youtube.com/watch?v=7xSfLPD6tiQ) a shout-out.

I hope that's the talk I'm thinking of anyway ... at the time of writing I'm too lazy to rewatch it :-)

At some point though Rob made the remark about, when in doubt, parameterize the types. And I've found that generally served a good rule,
and so is what I did when represending the underlying nodes in the cluster.

I introduced one parameter type A for the log type, which will always be there. You should be able to use the Raft protocol
with any message type. The second param type I had was 'NodeKey'.

Essentially I didn't care what the underlying nodes were in the cluster. They could've been ActorRefs, Observables, WebSockets,
Http clients, whatever. And so I just made them a parameterized NodeKey, thinking the extra cost of passing that type around (and
exposing it in the interfaces) would be useful when it came time to interact w/ the types in riff-core.

Well, I should've gone full end-to-end sooner (see the previous post about tracer-bullets), because it turns out its not so useful.

It was a minor thing I suppose -- the [PersistentState](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/node/PersistentState.html) needs
to write down who it voted for to avoid potentially double-voting after a crash/restart during an election.

If we kept the parameterized NodeKey, that means we have to be able either serialize/deserialize the key itself, or provide some
momento for that NodeKey. So the idea of making the RaftNode hold a reference to a generic NodeKey started to lose its appeal when
having to provide additional support for serialisation.

And so, upon reevaluation, just making it become a string identifier in 'NodeId' was a lot more appealing. We could drop the extra
param type tax and simplify things. It should be trivial for implementations to resolve a transport for a unique name.

And, as for unique names, that should be a little more clear now that it's not a generic NodeKey but a specific NodeId (i.e. String).
