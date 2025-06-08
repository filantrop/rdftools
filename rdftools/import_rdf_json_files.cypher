//:param importPath=>"file:/C:/git_axintro/rdftools/tests/RDF-Model-Syntax_1.0"


// 1.
with $importPath as importPath
return importPath
;
match (n) detach delete n
;
// #2
with $importPath as importPath
with "file:/"+importPath as importPath
call apoc.load.directory('*.*',importPath,{recursive: false}) yield value

with replace(value,"\\","/")
as filePath


call apoc.load.json("file:///"+filePath)
yield value


with *,apoc.text.regexGroups(filePath, '.*/([^/]+)$')[0][1] AS fileName
// CREATE (f:RdfFile {filePath:filePath, fileName: fileName })
// with *
unwind keys(value) as key
    with *, value[key] as val
    with *,{fileName:fileName,Context:'RDF_CONTEXT',filePath:filePath,label:key,fields: coalesce(val,["EMPTY"])} as output

    call apoc.graph.fromDocument(output,{generateID:true,skipValidation:true,write:true})
    yield graph as g

    optional call (g) {
        unwind g.nodes as d
            match ()-[r]->(d)
            with r,type(r) as t,size(type(r)) as length
            with t,r,
            case
            when t ends with 'S' then left(t,length-1)
            else t
            end as label
            with r,apoc.text.upperCamelCase(label) as l
            with *, endNode(r) as end
            set end:$(l),end.Context='RDF_CONTEXT'
    }

return count(distinct g) as `Antal jsonfiler`