module Thor
  class CSDLHelpers

    def self.generate(projects)
      constraints = 'interaction.type == "twitter"'
      tag_set     = ""
      filter_set  = ""

      all_authors    = []
      all_hashtags   = []
      all_keywords   = []
      all_casewords  = []
      all_keyphrases = []

      projects.each do |p|
        p.topics.where(active: true).each do |t|

          unless t.authors.empty?
            all_authors += authors = t.serialize(:authors)
            tag_set += "tag.#{p.tag}.#{t.tag} \"authors\" { interaction.author.username contains_any \"#{authors.join(', ')}\" } "
          end # unless t.authors.empty?

          unless t.hashtags.empty?
            all_hashtags += hashtags = t.serialize(:hashtags)
            tag_set += "tag.#{p.tag}.#{t.tag} \"hashtags\" { interaction.hashtags contains_any \"#{hashtags.join(', ')}\" } "
          end # unless t.hashtags.empty?

          unless t.keywords.empty?
            all_keywords   += keywords   = t.serialize(:keywords)
            all_casewords  += casewords  = t.serialize(:casewords)
            all_keyphrases += keyphrases = t.serialize(:keyphrases)

            word_string = ""
            unless keywords.empty?
              word_string += "interaction.content contains_any  \"#{keywords.join(', ')}\" "
              unless casewords.empty? && keyphrases.empty?
                word_string += "OR "
              end
            end

            unless casewords.empty?
              word_string += casewords.map{|cw| "interaction.content cs contains \"#{cw}\""}.join(" OR ") + " "
              unless keyphrases.empty?
                word_string += "OR "
              end
            end

            unless keyphrases.empty?
              word_string += keyphrases.map{|kp| "interaction.content contains \"#{kp}\""}.join(" OR ")
            end

            tag_set += "tag.#{p.tag}.#{t.tag} \"keywords\" { #{word_string} }"
          end # unless t.keywords.empty?

        end
      end




      unless all_authors.empty?
        filter_set += "interaction.author.username contains_any \"#{all_authors.uniq.join(', ')}\" "
        unless all_hashtags.empty? && all_keywords.empty? && all_casewords.empty? && all_keyphrases.empty?
          filter_set += " OR "
        end
      end

      unless all_hashtags.empty?
        filter_set += "interaction.hashtags contains_any \"#{all_hashtags.uniq.join(', ')}\" "
        unless all_keywords.empty? && all_casewords.empty? && all_keyphrases.empty?
          filter_set += " OR "
        end
      end

      unless all_keywords.empty?
        filter_set += "interaction.content contains_any \"#{all_keywords.uniq.join(', ')}\" "
        unless all_casewords.empty? && all_keyphrases.empty?
          filter_set += " OR "
        end
      end

      unless all_casewords.empty?
        filter_set += all_casewords.uniq.map{|cw| "interaction.content cs contains \"#{cw}\""}.join(" OR ") + " "
        unless all_keyphrases.empty?
          filter_set += " OR "
        end
      end

      unless all_keyphrases.empty?
        filter_set += all_keyphrases.uniq.map{|kp| "interaction.content contains \"#{kp}\""}.join(" OR ")
      end

      definition = "#{tag_set} return {(#{constraints}) AND (#{filter_set})}"

    end

  end # class CSDLHelpers
end # module Thor
