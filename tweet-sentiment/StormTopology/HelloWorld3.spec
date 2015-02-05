{
  :name "HelloWorld3"
  :topology
    (nontx-topolopy
      "HelloWorld3"

      {
        "generator" 

        (spout-spec 
          (scp-spout
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["generator"]
              "output.schema" {"default" ["sentence"]}
            })
           
          :p 1)
      }

      {
        "splitter"  

        (bolt-spec
          {
            "generator" :shuffle
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["splitter"]
              "output.schema" {"default" ["word"]}
            })

          :p 1)

        "counter"  

        (bolt-spec
          {
            "splitter" :global
          }

          (scp-bolt
            {
              "plugin.name" "StormTopology.exe"
              "plugin.args" ["counter"]
              "output.schema" {"default" ["word" "count"]}
            })

          :p 1)
      })

  :config
    {
      "topology.kryo.register" ["[B"]
    }
}
