
Having had a look at the offerings, it seemed the existing JVM Raft implementations were all tied to a framework or paradigm
(typically actors).

I'm of the opinion that busiess logic should be writtin in as vanilla a way possible. Just pure functions ideally, which are then "lifted" into some other framework
(actors, streams, REST services, whatever).

The reasoning is largely (hopefully) to aid in readability and reuse, the first of which at least is a core tennent of why Raft was designed the way it is in the first place.

I also find that the public-facing APIs tend to be in the form:
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

I started this project because
 * I found the existing Raft libs for Java a little lacking
 * I wanted to play around with different approaches (free monads, finally tagless, etc)
 * I wanted to showcase a way to keep concerns like data transport/delivery/consensus separate from other business logic, as well as ways it can be tested/demonstrated
   in a (hopefully) easy-to-follow, repeatable way
 * I fancied a place where I can easily add submodules for interesting/compelling new libraries. E.g. instead of have a 'hello world' for http4s, akka streams, monix, etc,
   Here is a small, well defined, real-world project which can be driven by those nifty libraries.

It will hopefully be useful, at least to me, and I look forward to the "did you consider" or "you seem to have neglected..." comments