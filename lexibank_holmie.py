from pathlib import Path
from pylexibank.dataset import Dataset as BaseDataset 
from pylexibank import Language, Concept, FormSpec
from pylexibank import progressbar


from lingpy import Wordlist
from clldutils.misc import slug
import attr


@attr.s
class CustomLanguage(Language):
    Name_in_Source = attr.ib(default=None)


@attr.s
class CustomConcept(Concept):
    Glosses_in_Source = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "holmie"
    language_class = CustomLanguage
    concept_class = CustomConcept
    form_spec = FormSpec(
            missing_data=("-", ),
            separators="/,;",
            replacements=[(" ", "_")],
            strip_inside_brackets=False,
            first_form_only=True,
            brackets={},
            )

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.
        """
        concepts, wl_concepts = {}, {}
        visited = set()
        for concept in self.concepts:
            cid = '{0}_{1}'.format(concept['NUMBER'], slug(concept['ENGLISH']))
            if cid in visited:
                pass
            else:
                visited.add(cid)
                args.writer.add_concept(
                        ID=cid,
                        Name=concept['ENGLISH'],
                        Glosses_in_Source=concept['GLOSSES_IN_SOURCE'],
                        Concepticon_ID=concept['CONCEPTICON_ID'],
                        Concepticon_Gloss=concept['CONCEPTICON_GLOSS']
                        )
                for gloss in concept['GLOSSES_IN_SOURCE'].split(' // '):
                    concepts[gloss] = cid
                    wl_concepts[gloss] = concept['ENGLISH']

                
        languages = args.writer.add_languages(lookup_factory="Name_in_Source")
        args.writer.add_sources()

        # make a wordlist for edictor to inspect the data
        D = {0: ['doculect', 'concept', 'ipa', 'cogid']}
        idx = 1

        for i, row in progressbar(
                enumerate(
                    self.raw_dir.read_csv(
                        'data.tsv', delimiter='\t', dicts=True))):
            for language, lid in languages.items():
                form = row[language].strip()
                if form:
                    lexemes = args.writer.add_forms_from_value(
                            Language_ID=lid,
                            Parameter_ID=concepts[row['Meaning']],
                            Value=form,
                            Source='Holm2017'
                            )
                    if lexemes:
                        args.writer.add_cognate(
                                lexeme=lexemes[0],
                                Cognateset_ID=str(i+1),
                                Cognate_Detection_Method='expert',
                                Source='Holm2017'
                                )
                        D[idx] = [language, wl_concepts[row['Meaning']], form, i+1]
                        idx += 1
        Wordlist(D).output(
                'tsv',
                filename=self.raw_dir.joinpath('wordlist').as_posix())




