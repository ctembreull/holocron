module VendorX
  module Provider
    class Datasift < HashPath
      path :tweet_id,              "twitter/id"
      path :author_id,             "interaction/author/username"
      path :mentions,              "interaction/mentions"
      path :tag_tree,              "interaction/tag_tree"
      path :created_at,            "interaction/created_at"

      path :is_retweet,            "twitter/retweet"

      path :reply_author_id,       "twitter/in_reply_to_user_id"

      path :tweet_author,          "twitter/user"
      path :tweet_author_name,     "twitter/user/screen_name"
      path :tweet_created_at,      "twitter/created_at"

      path :retweet_author,        "twitter/retweet/user"
      path :retweet_author_name,   "twitter/retweet/user/screen_name"
      path :retweet_created_at,    "twitter/retweet/created_at"

      path :retweeted_author,      "twitter/retweeted/user"
      path :retweeted_author_name, "twitter/retweeted/user/screen_name"
      path :retweeted_created_at,  "twitter/retweeted/created_at"

      def is_retweet?
        !is_retweet.nil?
      end

      def is_reply?
        !reply_author_id.nil?
      end

      def has_mentions?
        !mentions.nil?
      end

    end
  end
end
