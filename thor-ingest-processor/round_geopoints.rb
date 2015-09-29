module VendorX
  module Transform
    class RoundGeopoints

      def self.process(doc)
        begin

          unless doc['interaction']['geo'].nil?
            doc['interaction']['geo']['latitude']  = '%.5f' % doc['interaction']['geo']['latitude']
            doc['interaction']['geo']['longitude'] = '%.5f' % doc['interaction']['geo']['longitude']
          end

          if doc.is_retweet?
            unless doc['twitter']['retweet']['geo'].nil?
              doc['twitter']['retweet']['geo']['latitude']  = '%.5f' % doc['twitter']['retweet']['geo']['latitude']
              doc['twitter']['retweet']['geo']['longitude'] = '%.5f' % doc['twitter']['retweet']['geo']['longitude']
            end
            unless doc['twitter']['retweeted']['geo'].nil?
              doc['twitter']['retweeted']['geo']['latitude']  = '%.5f' % doc['twitter']['retweeted']['geo']['latitude']
              doc['twitter']['retweeted']['geo']['longitude'] = '%.5f' % doc['twitter']['retweeted']['geo']['longitude']
            end
          else
            unless doc['twitter']['geo'].nil?
              doc['twitter']['geo']['latitude']  = '%.5f' % doc['twitter']['geo']['latitude']
              doc['twitter']['geo']['longitude'] = '%.5f' % doc['twitter']['geo']['longitude']
            end
          end
        rescue NoMethodError => e
          # fail nicely
        end

        doc
      end

    end
  end
end
