# Beads

Query language for event chains. 
- Allows you to create conditions based on other events declared in the query
- Finds all occurrences, and not only the first one
- Was tested on 100k events only. Not very scalable yet
- You don't need to deploy any additional database or broker - just use it with your Pandas code

# Example

Find session segments with connection error during intro cutscene, and the end of cutscene was delayed, presumably because of this error.
```
session_start
=> A: IntroStarted
=> and {
  not {IntroCompleted}
} {ConnectionError}
=> IntroCompleted[this.time - A.time > 60]
```

You are gladly welcome in this [Quickstart](https://github.com/snail-fuji/beads/blob/master/query-showcase.ipynb) notebook, if you are interested!
