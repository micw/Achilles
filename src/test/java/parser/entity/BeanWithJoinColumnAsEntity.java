package parser.entity;

import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;

import fr.doan.achilles.entity.type.WideRow;

/**
 * BeanWithJoinColumnAsEntity
 * 
 * @author DuyHai DOAN
 * 
 */
@Table
public class BeanWithJoinColumnAsEntity
{
	@Id
	private Long id;

	@JoinColumn
	private WideRow<Integer, Bean> wide;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public WideRow<Integer, Bean> getWide()
	{
		return wide;
	}

	public void setWide(WideRow<Integer, Bean> wide)
	{
		this.wide = wide;
	}
}
