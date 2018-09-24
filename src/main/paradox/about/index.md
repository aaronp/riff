#About 

@@@ index

* [Getting Started](gettingStarted.md)
* [Scaladoc](api.md)

@@@


This started out with just an interest in Raft. I thought it'd be a good candidate to play around with trying out different approaches, as it is easy to understand, well specified, and also useful! It's also a nice mix of networked communication, disk IO, etc, and so I could get a feel for trying to use free monads, finally tagless, or whatever the latest "ooh, how will this work in the real world" technology.

It's grown a bit from that, as I found the existing JVM implementations a bit lacking, if only because they were all coupled w/ a framework or library, such as akka.

I'm of the opinion that busiess logic should be writtin in as vanilla a way possible. Just pure functions ideally, which are then "lifted" into some other framework (actors, streams, REST services, whatever).

And so, the reasoning is largely (hopefully) to aid in readability and reuse, the first of which at least is a core tennent of why Raft was designed the way it is in the first place.


## General approach

I find most interfaces can be reduced to something in the form:
```
SomeInput : Foo | Bar(x) | Bazz(y, z)
SomeOutput : Ok | ResultOne(a) | ResultTwo(b)
```

In scala, you can then can make this easier to write by exposing a trait like this:
```scala
trait SomethingDsl {

	def onInput(input : SomeInput) : SomeOutput

	final def foo() = onInput(Foo)

	final def bar(x : Int)  = onInput(Bar(x))

	...
}

```

or even go (arguably dangerously) circular, leaving which implementation to specify up to the thing implementing it:

```scala
trait SomethingDsl {

	def onInput(input : SomeInput) : SomeOutput = input match {
		case Foo => foo()
		case Bar(x) => bar(x)
		...
	}

	def foo() = onInput(Foo)

	def bar(x : Int)  = onInput(Bar(x))

	...
}

```

And by representing the inputs using the command pattern, it then easily lends itself to making those commands serialise to/from json (or some binary protocol) for use over the wire. Or, if not, they're messages which can be put on a queue or sent to an actor, so we go a ways already by representing things in that form.

And so that's the approach I've taken with [RaftNode](https://aaronp.github.io/riff/api/riffCoreCrossProject/riff/raft/node/RaftNode.html)). It has an 'onMessage' which returns a result, and so can easily be put behind a REST service, akka actor, queue, etc.

So, that's it. It will hopefully be useful, at least to me, and I look forward to the "did you consider" or "you seem to have neglected..." comments.