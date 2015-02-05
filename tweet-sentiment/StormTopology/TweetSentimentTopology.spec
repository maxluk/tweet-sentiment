{
  :name "TweetSentimentTopology"
  :topology
    (nontx-topolopy
      "TweetSentimentTopology"

      {
        "spout" 

        (spout-spec 
          (scp-spout
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["twitterspout"]
              "output.schema" {"default" ["text" "createdat" "idstr" "lang" "coor"]}
            })
           
          :p 1)
      }

      {
        "sentimentindexer"

        (bolt-spec
          {
            "spout" :shuffle
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["sentimentindexerbolt"]
			  "output.schema" {"default" ["word" "createdat" "idstr" "lang" "coor" "sentimentscore"]}
            })

          :p 8)

        "writer"

        (bolt-spec
          {
            "sentimentindexer" :shuffle
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["hbasewriterbolt"]
            })

          :p 8)

        "counter"

        (bolt-spec
          {
            "sentimentindexer" :global
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["hbasecounterbolt"]
            })

          :p 1)

        "broadcaster"

        (bolt-spec
          {
            "sentimentindexer" :all
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["signalrbroadcastbolt"]
            })

          :p 4)

      })

  :config
    {
	  "topology.workers" 8
      "topology.kryo.register" ["[B"]
    }
}
