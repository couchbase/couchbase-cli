Asciidoctor::Extensions.register do
  inline_macro :man do
    name_positional_attributes 'volnum'
    process do |parent, target, attrs|
      manname = target
      suffix = (volnum = attrs['volnum']) ? %[(#{volnum})] : ''
      if (doc = parent.document).basebackend? 'html'
        create_anchor parent, %(#{manname}#{suffix}), type: :link, target: %(#{manname}#{doc.outfilesuffix})
      elsif doc.backend == 'manpage'
        doc.sub_quotes %(**#{manname}**#{suffix})
      else
        %(#{manname}#{suffix})
      end
    end
  end
end
